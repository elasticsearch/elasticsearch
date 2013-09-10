/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.rest.action.admin.indices.flush;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;
import static org.elasticsearch.common.Strings.splitValues;

/**
 *
 */
public class RestFlushAction extends BaseRestHandler {

    @Inject
    public RestFlushAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(POST, "/_flush", this);
        controller.registerHandler(POST, "/{index}/_flush", this);

        controller.registerHandler(GET, "/_flush", this);
        controller.registerHandler(GET, "/{index}/_flush", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        FlushRequest flushRequest = new FlushRequest(splitValues(request.param("index")));
        flushRequest.listenerThreaded(false);
        if (request.hasParam("ignore_indices")) {
            flushRequest.ignoreIndices(IgnoreIndices.fromString(request.param("ignore_indices")));
        }
        BroadcastOperationThreading operationThreading = BroadcastOperationThreading.fromString(request.param("operationThreading"), BroadcastOperationThreading.THREAD_PER_SHARD);
        if (operationThreading == BroadcastOperationThreading.NO_THREADS) {
            // since we don't spawn, don't allow no_threads, but change it to a single thread
            operationThreading = BroadcastOperationThreading.THREAD_PER_SHARD;
        }
        flushRequest.operationThreading(operationThreading);
        flushRequest.refresh(request.paramAsBoolean("refresh", flushRequest.refresh()));
        flushRequest.full(request.paramAsBoolean("full", flushRequest.full()));
        flushRequest.force(request.paramAsBoolean("force", flushRequest.force()));
        client.admin().indices().flush(flushRequest, new ActionListener<FlushResponse>() {
            @Override
            public void onResponse(FlushResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    builder.field("ok", true);

                    buildBroadcastShardsHeader(builder, response);

                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Throwable e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}
