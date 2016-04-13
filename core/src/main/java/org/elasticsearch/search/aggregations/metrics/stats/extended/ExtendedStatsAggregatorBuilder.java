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

package org.elasticsearch.search.aggregations.metrics.stats.extended;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;

import java.io.IOException;
import java.util.Objects;

public class ExtendedStatsAggregatorBuilder
        extends ValuesSourceAggregatorBuilder.LeafOnly<ValuesSource.Numeric, ExtendedStatsAggregatorBuilder> {

    static final ExtendedStatsAggregatorBuilder PROTOTYPE = new ExtendedStatsAggregatorBuilder("");

    private double sigma = 2.0;

    public ExtendedStatsAggregatorBuilder(String name) {
        super(name, InternalExtendedStats.TYPE, ValuesSourceType.NUMERIC, ValueType.NUMERIC);
    }

    public ExtendedStatsAggregatorBuilder sigma(double sigma) {
        if (sigma < 0.0) {
            throw new IllegalArgumentException("[sigma] must be greater than or equal to 0. Found [" + sigma + "] in [" + name + "]");
        }
        this.sigma = sigma;
        return this;
    }

    public double sigma() {
        return sigma;
    }

    @Override
    protected ExtendedStatsAggregatorFactory innerBuild(AggregationContext context, ValuesSourceConfig<Numeric> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        return new ExtendedStatsAggregatorFactory(name, type, config, sigma, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected ExtendedStatsAggregatorBuilder innerReadFrom(String name, ValuesSourceType valuesSourceType,
            ValueType targetValueType, StreamInput in) throws IOException {
        ExtendedStatsAggregatorBuilder factory = new ExtendedStatsAggregatorBuilder(name);
        factory.sigma = in.readDouble();
        return factory;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(sigma);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(ExtendedStatsAggregator.SIGMA_FIELD.getPreferredName(), sigma);
        return builder;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(sigma);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        ExtendedStatsAggregatorBuilder other = (ExtendedStatsAggregatorBuilder) obj;
        return Objects.equals(sigma, other.sigma);
    }
}