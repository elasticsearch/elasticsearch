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

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.snapshots.features.GetSnapshottableFeaturesRequest;
import org.elasticsearch.action.admin.cluster.snapshots.features.SnapshottableFeaturesAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

public class RestSnapshottableFeaturesAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_snapshottable_features"));
    }

    @Override
    public String getName() {
        return "get_snapshottable_features";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final GetSnapshottableFeaturesRequest req = new GetSnapshottableFeaturesRequest();
        req.masterNodeTimeout(request.paramAsTime("master_timeout", req.masterNodeTimeout()));

        return restChannel -> {
            client.execute(SnapshottableFeaturesAction.INSTANCE, req, new RestToXContentListener<>(restChannel));
        };
    }
}
