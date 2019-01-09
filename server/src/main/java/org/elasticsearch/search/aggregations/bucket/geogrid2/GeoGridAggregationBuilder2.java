/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.geogrid2;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.MultiBucketAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class GeoGridAggregationBuilder2 extends ValuesSourceAggregationBuilder<ValuesSource.GeoPoint, GeoGridAggregationBuilder2>
    implements MultiBucketAggregationBuilder {
    public static final String NAME = "geo_grid";
    public static final int DEFAULT_MAX_NUM_CELLS = 10000;

    /* recognized field names in JSON */
    static final ParseField FIELD_TYPE = new ParseField("hash_type");
    static final ParseField FIELD_PRECISION = new ParseField("precision");
    static final ParseField FIELD_SIZE = new ParseField("size");
    static final ParseField FIELD_SHARD_SIZE = new ParseField("shard_size");

    public static GeoGridTypes types;

    private static final ConstructingObjectParser<GeoGridAggregationBuilder2, String> PARSER;

    static {
        types = GeoGridTypes.DEFAULT;

        PARSER = new ConstructingObjectParser<>(GeoGridAggregationBuilder2.NAME, false,
            (a, name) -> new GeoGridAggregationBuilder2(name, (GeoGridType) a[0]));

        PARSER.declareField(
            constructorArg(),
            GeoGridAggregationBuilder2::parseType,
            FIELD_TYPE,
            ObjectParser.ValueType.STRING);
        PARSER.declareField(
            GeoGridAggregationBuilder2::precisionRaw,
            GeoGridAggregationBuilder2::parsePrecision,
            FIELD_PRECISION,
            ObjectParser.ValueType.VALUE);
        PARSER.declareInt(
            GeoGridAggregationBuilder2::size,
            FIELD_SIZE);
        PARSER.declareInt(
            GeoGridAggregationBuilder2::shardSize,
            FIELD_SHARD_SIZE);

        ValuesSourceParserHelper.declareGeoFields(PARSER, false, false);
    }

    private static Object parsePrecision(XContentParser parser, String name)
        throws IOException {
        // Delay actual parsing until builder.precision()
        // In some cases, this value cannot be fully parsed until after we know the type
        final XContentParser.Token token = parser.currentToken();
        switch (token) {
            case VALUE_NUMBER:
                return parser.intValue();
            case VALUE_STRING:
                return parser.text();
            default:
                throw new XContentParseException(parser.getTokenLocation(),
                    "[geo_grid] failed to parse field [precision] in [" + name +
                        "]. It must be either an integer or a string");
        }
    }

    public static GeoGridAggregationBuilder2 parse(String aggregationName, XContentParser parser) {
        return PARSER.apply(parser, aggregationName);
    }

    private final GeoGridType type;
    private int precision;
    private int requiredSize = DEFAULT_MAX_NUM_CELLS;
    private int shardSize = -1;

    public GeoGridAggregationBuilder2(String name, GeoGridType type) {
        super(name, ValuesSourceType.GEOPOINT, ValueType.GEOPOINT);
        this.type = type;
        this.precision = this.type.getDefaultPrecision();
    }

    protected GeoGridAggregationBuilder2(GeoGridAggregationBuilder2 clone, Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.type = clone.type;
        this.precision = clone.precision;
        this.requiredSize = clone.requiredSize;
        this.shardSize = clone.shardSize;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new GeoGridAggregationBuilder2(this, factoriesBuilder, metaData);
    }

    /**
     * Read from a stream.
     */
    public GeoGridAggregationBuilder2(StreamInput in) throws IOException {
        super(in, ValuesSourceType.GEOPOINT, ValueType.GEOPOINT);

        // FIXME: better debug name than a class name?
        type = types.get(in.readString(), this.getClass().getName());
        precision = in.readVInt();
        requiredSize = in.readVInt();
        shardSize = in.readVInt();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeString(type.getName());
        out.writeVInt(precision);
        out.writeVInt(requiredSize);
        out.writeVInt(shardSize);
    }

    private static GeoGridType parseType(XContentParser parser, String name) throws IOException {
        return types.get(parser.text(), name);
    }

    public GeoGridType type() {
        return type;
    }

    private GeoGridAggregationBuilder2 precisionRaw(Object precision) {
        if (precision == null) {
            this.precision(type.getDefaultPrecision());
        } else if (precision instanceof String) {
            this.precision(type.parsePrecisionString((String) precision));
        } else {
            this.precision((int) precision);
        }
        return this;
    }

    public GeoGridAggregationBuilder2 precision(int precision) {
        this.precision = type.validatePrecision(precision);
        return this;
    }

    public int precision() {
        return precision;
    }

    public GeoGridAggregationBuilder2 size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException(
                "[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
        }
        this.requiredSize = size;
        return this;
    }

    public int size() {
        return requiredSize;
    }

    public GeoGridAggregationBuilder2 shardSize(int shardSize) {
        if (shardSize <= 0) {
            throw new IllegalArgumentException(
                "[shardSize] must be greater than 0. Found [" + shardSize + "] in [" + name + "]");
        }
        this.shardSize = shardSize;
        return this;
    }

    public int shardSize() {
        return shardSize;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource.GeoPoint, ?> innerBuild(SearchContext context,
            ValuesSourceConfig<ValuesSource.GeoPoint> config, AggregatorFactory<?> parent, Builder subFactoriesBuilder)
                    throws IOException {
        int shardSize = this.shardSize;
        int requiredSize = this.requiredSize;

        if (shardSize < 0) {
            // Use default heuristic to avoid any wrong-ranking caused by
            // distributed counting
            shardSize = BucketUtils.suggestShardSideQueueSize(requiredSize);
        }
        if (requiredSize <= 0 || shardSize <= 0) {
            throw new ElasticsearchException(
                "parameters [required_size] and [shard_size] must be >0 in geo_grid aggregation [" + name + "].");
        }
        if (shardSize < requiredSize) {
            shardSize = requiredSize;
        }
        return new GeoGridAggregatorFactory(name, config, type, precision, requiredSize, shardSize, context, parent,
            subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_TYPE.getPreferredName(), type.getName());
        builder.field(FIELD_PRECISION.getPreferredName(), precision);
        builder.field(FIELD_SIZE.getPreferredName(), requiredSize);
        if (shardSize > -1) {
            builder.field(FIELD_SHARD_SIZE.getPreferredName(), shardSize);
        }
        return builder;
    }

    @Override
    protected boolean innerEquals(Object obj) {
        GeoGridAggregationBuilder2 other = (GeoGridAggregationBuilder2) obj;
        return Objects.equals(type, other.type) &&
            Objects.equals(precision, other.precision) &&
            Objects.equals(requiredSize, other.requiredSize) &&
            Objects.equals(shardSize, other.shardSize);
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(type, precision, requiredSize, shardSize);
    }

    @Override
    public String getType() {
        return NAME;
    }

    private static class CellValues extends AbstractSortingNumericDocValues {
        private MultiGeoPointValues geoValues;
        private GeoGridType type;
        private int precision;

        protected CellValues(MultiGeoPointValues geoValues, GeoGridType type, int precision) {
            this.geoValues = geoValues;
            this.type = type;
            this.precision = precision;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            if (geoValues.advanceExact(docId)) {
                resize(geoValues.docValueCount());

                for (int i = 0; i < docValueCount(); ++i) {
                    GeoPoint target = geoValues.nextValue();
                    values[i] = type.calculateHash(target.getLon(), target.getLat(), precision);
                }
                sort();
                return true;
            } else {
                return false;
            }
        }
    }

    static class CellIdSource extends ValuesSource.Numeric {
        private final ValuesSource.GeoPoint valuesSource;
        private final GeoGridType type;
        private final int precision;

        CellIdSource(ValuesSource.GeoPoint valuesSource, GeoGridType type, int precision) {
            this.valuesSource = valuesSource;
            //different GeoPoints could map to the same or different geogrid cells.
            this.type = type;
            this.precision = precision;
        }

        public GeoGridType type() {
            return type;
        }

        public int precision() {
            return precision;
        }

        @Override
        public boolean isFloatingPoint() {
            return false;
        }

        @Override
        public SortedNumericDocValues longValues(LeafReaderContext ctx) {
            return new CellValues(valuesSource.geoPointValues(ctx), type, precision);
        }

        @Override
        public SortedNumericDoubleValues doubleValues(LeafReaderContext ctx) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext ctx) {
            throw new UnsupportedOperationException();
        }

    }
}
