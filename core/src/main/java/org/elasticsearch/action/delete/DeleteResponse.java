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

package org.elasticsearch.action.delete;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * The response of the delete action.
 *
 * @see org.elasticsearch.action.delete.DeleteRequest
 * @see org.elasticsearch.client.Client#delete(DeleteRequest)
 */
public class DeleteResponse extends DocWriteResponse {

    private static final String FOUND = "found";

    public DeleteResponse() {
    }

    public DeleteResponse(ShardId shardId, String type, String id, long seqNo, long version, boolean found) {
        super(shardId, type, id, seqNo, version, found ? Result.DELETED : Result.NOT_FOUND);
    }

    @Override
    public RestStatus status() {
        return result == Result.DELETED ? super.status() : RestStatus.NOT_FOUND;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DeleteResponse[");
        builder.append("index=").append(getIndex());
        builder.append(",type=").append(getType());
        builder.append(",id=").append(getId());
        builder.append(",version=").append(getVersion());
        builder.append(",result=").append(getResult().getLowercase());
        builder.append(",shards=").append(getShardInfo());
        return builder.append("]").toString();
    }

    @Override
    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FOUND, result == Result.DELETED);
        super.innerToXContent(builder, params);
        return builder;
    }

    public static DeleteResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

        DeleteResponseBuilder context = new DeleteResponseBuilder();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parseXContentFields(parser, context);
        }
        return context.build();
    }

    /**
     * Parse the current token and update the parsing context appropriately.
     */
    public static void parseXContentFields(XContentParser parser, DeleteResponseBuilder context) throws IOException {
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = parser.currentName();

        if (FOUND.equals(currentFieldName)) {
            if (token.isValue()) {
                context.setFound(parser.booleanValue());
            }
        } else {
            DocWriteResponse.parseInnerToXContent(parser, context);
        }
    }

    public static class DeleteResponseBuilder extends DocWriteResponse.DocWriteResponseBuilder {

        private boolean found = false;

        public void setFound(boolean found) {
            this.found = found;
        }

        @Override
        public DeleteResponse build() {
            DeleteResponse deleteResponse = new DeleteResponse(shardId, type, id, seqNo, version, found);
            deleteResponse.setForcedRefresh(forcedRefresh);
            if (shardInfo != null) {
                deleteResponse.setShardInfo(shardInfo);
            }
            return deleteResponse;
        }
    }
}
