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
package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A factory that knows how to create an {@link PipelineAggregator} of a
 * specific type.
 */
public abstract class PipelineAggregatorFactory extends ToXContentToBytes implements NamedWriteable<PipelineAggregatorFactory>, ToXContent {

    protected String name;
    protected String type;
    protected String[] bucketsPaths;
    protected Map<String, Object> metaData;

    /**
     * Constructs a new pipeline aggregator factory.
     *
     * @param name
     *            The aggregation name
     * @param type
     *            The aggregation type
     */
    public PipelineAggregatorFactory(String name, String type, String[] bucketsPaths) {
        this.name = name;
        this.type = type;
        this.bucketsPaths = bucketsPaths;
    }

    public String name() {
        return name;
    }

    /**
     * Validates the state of this factory (makes sure the factory is properly
     * configured)
     *
     * @param pipelineAggregatorFactories
     * @param factories
     * @param parent
     */
    public final void validate(AggregatorFactory parent, AggregatorFactory[] factories,
            List<PipelineAggregatorFactory> pipelineAggregatorFactories) {
        doValidate(parent, factories, pipelineAggregatorFactories);
    }

    protected abstract PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException;

    /**
     * Creates the pipeline aggregator
     *
     * @param context
     *            The aggregation context
     * @param parent
     *            The parent aggregator (if this is a top level factory, the
     *            parent will be {@code null})
     * @param collectsFromSingleBucket
     *            If true then the created aggregator will only be collected
     *            with <tt>0</tt> as a bucket ordinal. Some factories can take
     *            advantage of this in order to return more optimized
     *            implementations.
     *
     * @return The created aggregator
     */
    public final PipelineAggregator create() throws IOException {
        PipelineAggregator aggregator = createInternal(this.metaData);
        return aggregator;
    }

    public void doValidate(AggregatorFactory parent, AggregatorFactory[] factories,
            List<PipelineAggregatorFactory> pipelineAggregatorFactories) {
    }

    public void setMetaData(Map<String, Object> metaData) {
        this.metaData = metaData;
    }

    public String getName() {
        return name;
    }

    public String[] getBucketsPaths() {
        return bucketsPaths;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        doWriteTo(out);
        out.writeStringArray(bucketsPaths);
        out.writeMap(metaData);
    }

    // NORELEASE make this abstract when agg refactor complete
    private void doWriteTo(StreamOutput out) {
    }

    // NORELEASE remove this method when agg refactor complete
    @Override
    public String getWriteableName() {
        return type;
    }

    @Override
    public PipelineAggregatorFactory readFrom(StreamInput in) throws IOException {
        String name = in.readString();
        String[] bucketsPaths = in.readStringArray();
        PipelineAggregatorFactory factory = doReadFrom(name, bucketsPaths, in);
        factory.metaData = in.readMap();
        return factory;
    }

    // NORELEASE make this abstract when agg refactor complete
    protected PipelineAggregatorFactory doReadFrom(String name, String[] bucketsPaths, StreamInput in) {
        return null;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());

        if (this.metaData != null) {
            builder.field("meta", this.metaData);
        }
        builder.startObject(type);

        if (bucketsPaths != null) {
            builder.startArray(PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName());
            for (String path : bucketsPaths) {
                builder.value(path);
            }
            builder.endArray();
        }

        internalXContent(builder, params);

        builder.endObject();

        return builder.endObject();
    }

    // NORELEASE make this method abstract when agg refactor complete
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(bucketsPaths);
        result = prime * result + ((metaData == null) ? 0 : metaData.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + doHashCode();
        return result;
    }

    // NORELEASE make this method abstract here when agg refactor complete (so
    // that subclasses are forced to implement it)
    protected int doHashCode() {
        throw new UnsupportedOperationException(
                "This method should be implemented by a sub-class and should not rely on this method. When agg re-factoring is complete this method will be made abstract.");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PipelineAggregatorFactory other = (PipelineAggregatorFactory) obj;
        if (!Arrays.equals(bucketsPaths, other.bucketsPaths))
            return false;
        if (metaData == null) {
            if (other.metaData != null)
                return false;
        } else if (!metaData.equals(other.metaData))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        return doEquals(obj);
    }

    // NORELEASE make this method abstract here when agg refactor complete (so
    // that subclasses are forced to implement it)
    protected boolean doEquals(Object obj) {
        throw new UnsupportedOperationException(
                "This method should be implemented by a sub-class and should not rely on this method. When agg re-factoring is complete this method will be made abstract.");
    }
}
