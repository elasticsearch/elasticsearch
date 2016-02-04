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

package org.elasticsearch.plugin.reindex;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response for the ReindexAction.
 */
public class ReindexResponse extends BulkIndexByScrollResponse {
    public ReindexResponse() {
    }

    public ReindexResponse(long took, BulkByScrollTask.Status status) {
        super(took, status);
    }

    public long getCreated() {
        return getStatus().getCreated();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("took", getTook());
        getStatus().innerXContent(builder, params, true, false);
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ReindexResponse[");
        builder.append("took=").append(getTook());
        getStatus().innerToString(builder, true, false);
        return builder.append(']').toString();
    }
}
