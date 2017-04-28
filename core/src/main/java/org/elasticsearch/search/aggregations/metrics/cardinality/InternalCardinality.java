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

package org.elasticsearch.search.aggregations.metrics.cardinality;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class InternalCardinality extends InternalNumericMetricsAggregation.SingleValue implements Cardinality {
    private final HyperLogLogPlusPlus counts;

    InternalCardinality(String name, HyperLogLogPlusPlus counts, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.counts = counts;
    }

    /**
     * Read from a stream.
     */
    public InternalCardinality(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        if (in.readBoolean()) {
            counts = HyperLogLogPlusPlus.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
        } else {
            counts = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        if (counts != null) {
            out.writeBoolean(true);
            counts.writeTo(0, out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public String getWriteableName() {
        return CardinalityAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public long getValue() {
        return counts == null ? 0 : counts.cardinality(0);
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        InternalCardinality reduced = null;
        for (InternalAggregation aggregation : aggregations) {
            final InternalCardinality cardinality = (InternalCardinality) aggregation;
            if (cardinality.counts != null) {
                if (reduced == null) {
                    reduced = new InternalCardinality(name, new HyperLogLogPlusPlus(cardinality.counts.precision(),
                            BigArrays.NON_RECYCLING_INSTANCE, 1), pipelineAggregators(), getMetaData());
                }
                reduced.merge(cardinality);
            }
        }

        if (reduced == null) { // all empty
            return aggregations.get(0);
        } else {
            return reduced;
        }
    }

    public void merge(InternalCardinality other) {
        assert counts != null && other != null;
        counts.merge(0, other.counts, 0);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final long cardinality = getValue();
        builder.field(CommonFields.VALUE.getPreferredName(), cardinality);
        return builder;
    }

    @Override
    protected int doHashCode() {
        return counts.hashCode(0);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalCardinality other = (InternalCardinality) obj;
        return counts.equals(0, other.counts);
    }

    HyperLogLogPlusPlus getState() {
        return counts;
    }
    private static final ObjectParser<Map<String, Object>, Void> PARSER = new ObjectParser<>(
            "internal_cardinality", true, () -> new HashMap<>());

    static {
        declareCommonField(PARSER);
        PARSER.declareLong((map, value) -> map.put(CommonFields.VALUE.getPreferredName(), value),
                CommonFields.VALUE);
    }

    public static Cardinality parseXContentBody(final String name, XContentParser parser) {
        Map<String, Object> map = PARSER.apply(parser, null);
        final long cardinalityValue = (Long) map.getOrDefault(CommonFields.VALUE.getPreferredName(),
                Double.POSITIVE_INFINITY);
        @SuppressWarnings("unchecked")
        final Map<String, Object> metaData = (Map<String, Object>) map
                .get(CommonFields.META.getPreferredName());
        return new ParsedCardinality(name, cardinalityValue, metaData);
    }

    private static class ParsedCardinality implements Aggregation, Cardinality, ToXContent {

        private long cardinalityValue;
        private String name;
        private Map<String, Object> medadata;

        ParsedCardinality(String name, long cardinality, Map<String, Object> metaData) {
            this.cardinalityValue = cardinality;
            this.name = name;
            this.medadata = metaData;
        }

        @Override
        public double value() {
            return getValue();
        }

        @Override
        public String getValueAsString() {
            return DocValueFormat.RAW.format(value());
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Object getProperty(String path) {
            if (path.isEmpty()) {
                return this;
            } else if ("value".equals(path)) {
                return value();
            } else {
                throw new IllegalArgumentException(
                        "path not supported for [" + getName() + "]: " + path);
            }
        }

        @Override
        public Map<String, Object> getMetaData() {
            return this.medadata;
        }

        @Override
        public long getValue() {
            return cardinalityValue;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(CommonFields.VALUE.getPreferredName(), cardinalityValue);
            return builder;
        }
    }
}

