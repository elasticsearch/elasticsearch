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


package org.elasticsearch.search.reducers.metric;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.reducers.ReductionBuilder;

import java.io.IOException;

public class MetricsBuilder extends ReductionBuilder<MetricsBuilder> {

    protected String path;
    protected String field;

    public MetricsBuilder(String name, String opName) {
        super(name, opName);
    }

    public MetricsBuilder bucketsPath(String path) {
        this.path = path;
        return this;
    }

    public MetricsBuilder field(String path) {
        this.field = path;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (path != null) {
            builder.field(MetricReducerParser.BUCKETS_FIELD.getPreferredName(), path);
        }

        if (field != null) {
            builder.field(MetricReducerParser.FIELD_NAME_FIELD.getPreferredName(), field);
        }
        buildCustomParameters(builder);
        builder.endObject();
        return builder;
    }

    protected XContentBuilder buildCustomParameters(XContentBuilder builder) throws IOException {
        return  builder;
    }
}

