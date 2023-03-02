/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.analytics;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

/**
 * The {@link AnalyticsCollection} model.
 */
public class AnalyticsCollection implements Writeable, ToXContentObject {

    private static final ObjectParser<AnalyticsCollection, String> PARSER = ObjectParser.fromBuilder(
        "analytics_collection",
        name -> new AnalyticsCollection(name)
    );

    public static final ParseField ANALYTICS_NAME_FIELD = new ParseField("name");
    public static final ParseField ANALYTICS_DATASTREAM_NAME_FIELD = new ParseField("datastream_name");

    private final String name;

    /**
     * Default public constructor.
     *
     * @param name Name of the analytics collection.
     */
    public AnalyticsCollection(String name) {
        this.name = Objects.requireNonNull(name);
    }

    /**
     * Build a new {@link AnalyticsCollection} from a stream.
     */
    public AnalyticsCollection(StreamInput in) throws IOException {
        this(in.readString());
    }

    /**
     * Getter for the {@link AnalyticsCollection} name.
     *
     * @return {@link AnalyticsCollection} name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * The event data stream used by the {@link AnalyticsCollection} to store events.
     * For now, it is a computed property because we have no real storage for the Analytics collection.
     *
     * @return Event data stream name/
     */
    public String getEventDataStream() {
        return AnalyticsTemplateRegistry.EVENT_DATA_STREAM_INDEX_PREFIX + name;
    }

    /**
     * Serialize the {@link AnalyticsCollection} to a XContent.
     *
     * @return Serialized {@link AnalyticsCollection}
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ANALYTICS_NAME_FIELD.getPreferredName(), this.getName());
        builder.field(ANALYTICS_DATASTREAM_NAME_FIELD.getPreferredName(), this.getEventDataStream());
        builder.endObject();

        return builder;
    }

    /**
     * Parses an {@link AnalyticsCollection} from its {@param xContentType} representation in bytes.
     *
     * @param resourceName The name of the resource (must match the {@link AnalyticsCollection} name).
     * @param source The bytes that represents the {@link AnalyticsCollection}.
     * @param xContentType The format of the representation.
     *
     * @return The parsed {@link AnalyticsCollection}.
     */
    public static AnalyticsCollection fromXContentBytes(String resourceName, BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return AnalyticsCollection.fromXContent(resourceName, parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
        }
    }

    /**
     * Parses an {@link AnalyticsCollection} through the provided {@param parser}.
     *
     * @param resourceName The name of the resource (must match the {@link AnalyticsCollection} name).
     * @param parser The {@link XContentType} parser.
     *
     * @return The parsed {@link AnalyticsCollection}.
     */
    public static AnalyticsCollection fromXContent(String resourceName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, resourceName);
    }

    public static AnalyticsCollection fromDataStreamName(String dataStreamName) {
        if (dataStreamName.startsWith(AnalyticsTemplateRegistry.EVENT_DATA_STREAM_INDEX_PREFIX) == false) {
            throw new IllegalArgumentException(
                "Data stream name (" + dataStreamName + " must start with " + AnalyticsTemplateRegistry.EVENT_DATA_STREAM_INDEX_PREFIX
            );
        }

        return new AnalyticsCollection(dataStreamName.replaceFirst(AnalyticsTemplateRegistry.EVENT_DATA_STREAM_INDEX_PREFIX, ""));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsCollection other = (AnalyticsCollection) o;
        return name.equals(other.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
