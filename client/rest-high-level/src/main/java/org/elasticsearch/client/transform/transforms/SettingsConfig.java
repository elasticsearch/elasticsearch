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

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SettingsConfig implements ToXContentObject {

    private static final ParseField MAX_PAGE_SEARCH_SIZE = new ParseField("max_page_search_size");
    private static final ParseField REQUESTS_PER_SECOND = new ParseField("requests_per_second");

    private final Integer maxPageSearchSize;
    private final Float requestsPerSecond;

    private static final ConstructingObjectParser<SettingsConfig, Void> PARSER = new ConstructingObjectParser<>(
        "settings_config",
        true,
        args -> new SettingsConfig((Integer) args[0], (Float) args[1])
    );

    static {
        PARSER.declareInt(optionalConstructorArg(), MAX_PAGE_SEARCH_SIZE);
        PARSER.declareFloat(optionalConstructorArg(), REQUESTS_PER_SECOND);
    }

    public static SettingsConfig fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    SettingsConfig(Integer maxPageSearchSize, Float requestsPerSecond) {
        this.maxPageSearchSize = maxPageSearchSize;
        this.requestsPerSecond = requestsPerSecond;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (maxPageSearchSize != null) {
            builder.field(MAX_PAGE_SEARCH_SIZE.getPreferredName(), maxPageSearchSize);
        }
        if (requestsPerSecond != null) {
            builder.field(REQUESTS_PER_SECOND.getPreferredName(), requestsPerSecond);
        }
        builder.endObject();
        return builder;
    }

    public Integer getMaxPageSearchSize() {
        return maxPageSearchSize;
    }

    public Float getRequestsPerSecond() {
        return requestsPerSecond;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other == null || other.getClass() != getClass()) {
            return false;
        }

        SettingsConfig that = (SettingsConfig) other;
        return Objects.equals(maxPageSearchSize, that.maxPageSearchSize) && Objects.equals(requestsPerSecond, that.requestsPerSecond);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxPageSearchSize, requestsPerSecond);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Integer maxPageSearchSize;
        private Float requestsPerSecond;

        /**
         * Sets the paging maximum paging maxPageSearchSize that transform can use when
         * pulling the data from the source index.
         *
         * If OOM is triggered, the paging maxPageSearchSize is dynamically reduced so that the transform can continue to gather data.
         *
         * @param maxPageSearchSize Integer value between 10 and 10_000
         * @return the {@link Builder} with the paging maxPageSearchSize set.
         */
        public Builder setMaxPageSearchSize(Integer maxPageSearchSize) {
            this.maxPageSearchSize = maxPageSearchSize;
            return this;
        }

        /**
         * Sets the requests per second that transform can use when pulling the data from the source index.
         *
         * This setting throttles transform by issuing queries less often, however processing still happens in
         * batches. A value of 0 disables throttling (default).
         *
         * @param requestsPerSecond Integer value
         * @return the {@link Builder} with requestsPerSecond set.
         */
        public Builder setRequestsPerSecond(Float requestsPerSecond) {
            this.requestsPerSecond = requestsPerSecond;
            return this;
        }

        public SettingsConfig build() {
            return new SettingsConfig(maxPageSearchSize, requestsPerSecond);
        }
    }
}
