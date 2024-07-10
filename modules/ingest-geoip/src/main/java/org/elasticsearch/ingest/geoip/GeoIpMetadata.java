/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.ingest.geoip.direct.DatabaseConfigurationMetadata;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Holds the geoip databases that are available in the cluster
 */
public final class GeoIpMetadata implements Metadata.Custom {

    public static final String TYPE = "geoip";
    private static final ParseField DATABASES_FIELD = new ParseField("database");

    public static final GeoIpMetadata EMPTY = new GeoIpMetadata(Map.of());

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GeoIpMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "ingest_metadata",
        a -> new GeoIpMetadata(
            ((List<DatabaseConfigurationMetadata>) a[1]).stream().collect(Collectors.toMap((m) -> m.database().id(), Function.identity()))
        )
    );
    static {
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> DatabaseConfigurationMetadata.parse(p, n), v -> {
            throw new IllegalArgumentException("ordered " + DATABASES_FIELD.getPreferredName() + " are not supported");
        }, DATABASES_FIELD);
    }

    private final Map<String, DatabaseConfigurationMetadata> databases;

    public GeoIpMetadata(Map<String, DatabaseConfigurationMetadata> databases) {
        this.databases = Map.copyOf(databases);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.MINIMUM_COMPATIBLE;
    }

    public Map<String, DatabaseConfigurationMetadata> getDatabases() {
        return databases;
    }

    public GeoIpMetadata(StreamInput in) throws IOException {
        {
            int size = in.readVInt();
            Map<String, DatabaseConfigurationMetadata> databases = Maps.newMapWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                DatabaseConfigurationMetadata databaseMeta = new DatabaseConfigurationMetadata(in);
                databases.put(databaseMeta.database().id(), databaseMeta);
            }
            this.databases = Map.copyOf(databases);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(databases.size());
        for (DatabaseConfigurationMetadata database : databases.values()) {
            database.writeTo(out);
        }
    }

    public static GeoIpMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.concat(ChunkedToXContentHelper.xContentValuesMap(DATABASES_FIELD.getPreferredName(), databases));
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom before) {
        return new GeoIpMetadataDiff((GeoIpMetadata) before, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new GeoIpMetadataDiff(in);
    }

    static class GeoIpMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, DatabaseConfigurationMetadata>> databases;

        GeoIpMetadataDiff(GeoIpMetadata before, GeoIpMetadata after) {
            this.databases = DiffableUtils.diff(before.databases, after.databases, DiffableUtils.getStringKeySerializer());
        }

        GeoIpMetadataDiff(StreamInput in) throws IOException {
            databases = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                DatabaseConfigurationMetadata::new,
                DatabaseConfigurationMetadata::readDiffFrom
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new GeoIpMetadata(databases.apply(((GeoIpMetadata) part).databases));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            databases.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.MINIMUM_COMPATIBLE;
        } // TODO ???
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeoIpMetadata that = (GeoIpMetadata) o;
        return Objects.equals(databases, that.databases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databases);
    }
}
