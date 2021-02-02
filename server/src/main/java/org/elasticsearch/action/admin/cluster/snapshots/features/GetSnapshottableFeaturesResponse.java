/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GetSnapshottableFeaturesResponse extends ActionResponse implements ToXContentObject {

    private final List<SnapshottableFeature> snapshottableFeatures;

    public GetSnapshottableFeaturesResponse(List<SnapshottableFeature> features) {
        this.snapshottableFeatures = Collections.unmodifiableList(features);
    }

    public GetSnapshottableFeaturesResponse(StreamInput in) throws IOException {
        super(in);
        snapshottableFeatures = in.readList(SnapshottableFeature::new);
    }

    public List<SnapshottableFeature> getSnapshottableFeatures() {
        return snapshottableFeatures;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(snapshottableFeatures);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startArray("features");
            for (SnapshottableFeature feature : snapshottableFeatures) {
                builder.value(feature);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GetSnapshottableFeaturesResponse)) return false;
        GetSnapshottableFeaturesResponse that = (GetSnapshottableFeaturesResponse) o;
        return snapshottableFeatures.equals(that.snapshottableFeatures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshottableFeatures);
    }

    public static class SnapshottableFeature implements Writeable, ToXContentObject {

        private final String featureName;
        private final String description;

        public SnapshottableFeature(String featureName, String description) {
            this.featureName = featureName;
            this.description = description;
        }

        public SnapshottableFeature(StreamInput in) throws IOException {
            featureName = in.readString();
            description = in.readString();
        }

        public String getFeatureName() {
            return featureName;
        }

        public String getDescription() {
            return description;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(featureName);
            out.writeString(description);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", featureName);
            builder.field("description", description);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SnapshottableFeature)) return false;
            SnapshottableFeature feature = (SnapshottableFeature) o;
            return Objects.equals(getFeatureName(), feature.getFeatureName());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getFeatureName());
        }
    }
}
