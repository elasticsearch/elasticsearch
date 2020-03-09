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

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestNodesHotThreadsAction extends BaseRestHandler {

    static final String formatDeprecatedMessageWithoutNodeID = "%s is a deprecated endpoint. Please use [/nodes/hot_threads] instead.";
    static final String formatDeprecatedMessageWithNodeID = "%s is a deprecated endpoint. Please use [/nodes/{nodeId}/hot_threads] instead.";
    static final String DEPRECATED_MESSAGE_CLUSTER_NODES_HOT_THREADS = String.format(formatDeprecatedMessageWithoutNodeID, "[/_cluster/nodes/hot_threads]"); 
    static final String DEPRECATED_MESSAGE_CLUSTER_NODES_NODEID_HOT_THREADS = String.format(formatDeprecatedMessageWithNodeID, "[/_cluster/nodes/{nodeId}/hot_threads]");
    static final String DEPRECATED_MESSAGE_CLUSTER_NODES_HOTTHREADS = String.format(formatDeprecatedMessageWithoutNodeID, "[/_cluster/nodes/hotthreads]"); 
    static final String DEPRECATED_MESSAGE_CLUSTER_NODES_NODEID_HOTTHREADS = String.format(formatDeprecatedMessageWithNodeID, "[/_cluster/nodes/{nodeId}/hotthreads]"); 
    static final String DEPRECATED_MESSAGE_NODES_HOTTHREADS = String.format(formatDeprecatedMessageWithoutNodeID, "[/_nodes/hotthreads]"); 
    static final String DEPRECATED_MESSAGE_NODES_NODEID_HOTTHREADS = String.format(formatDeprecatedMessageWithNodeID, "[/_nodes/{nodeId}/hotthreads]");

    @Override
    public List<DeprecatedRoute> deprecatedRoutes() {
        final String DEPRECATED_PATH_CLUSTER_NODES_HOT_THREADS = "/_cluster/nodes/hot_threads";
        final String DEPRECATED_PATH_CLUSTER_NODES_NODEID_HOT_THREADS = "/_cluster/nodes/{nodeId}/hot_threads";
        final String DEPRECATED_PATH_CLUSTER_NODES_HOTTHREADS = "/_cluster/nodes/hotthreads";
        final String DEPRECATED_PATH_CLUSTER_NODES_NODEID_HOTTHREADS = "/_cluster/nodes/{nodeId}/hotthreads";
        final String DEPRECATED_PATH_NODES_HOTTHREADS = "/_nodes/hotthreads";
        final String DEPRECATED_PATH_NODES_NODEID_HOTTHREADS = "/_nodes/{nodeId}/hotthreads";
        return List.of(
            new DeprecatedRoute(GET, DEPRECATED_PATH_CLUSTER_NODES_HOT_THREADS,
                    DEPRECATED_MESSAGE_CLUSTER_NODES_HOT_THREADS),
            new DeprecatedRoute(GET, DEPRECATED_PATH_CLUSTER_NODES_NODEID_HOT_THREADS,
                    DEPRECATED_MESSAGE_CLUSTER_NODES_NODEID_HOT_THREADS),
            new DeprecatedRoute(GET, DEPRECATED_PATH_CLUSTER_NODES_HOTTHREADS,
                    DEPRECATED_MESSAGE_CLUSTER_NODES_HOTTHREADS),
            new DeprecatedRoute(GET, DEPRECATED_PATH_CLUSTER_NODES_NODEID_HOTTHREADS,
                    DEPRECATED_MESSAGE_CLUSTER_NODES_NODEID_HOTTHREADS),
            new DeprecatedRoute(GET, DEPRECATED_PATH_NODES_HOTTHREADS,
                    DEPRECATED_MESSAGE_NODES_HOTTHREADS),
            new DeprecatedRoute(GET, DEPRECATED_PATH_NODES_NODEID_HOTTHREADS,
                    DEPRECATED_MESSAGE_NODES_NODEID_HOTTHREADS));
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_nodes/hot_threads"),
            new Route(GET, "/_nodes/{nodeId}/hot_threads")
        );
    }

    @Override
    public String getName() {
        return "nodes_hot_threads_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        NodesHotThreadsRequest nodesHotThreadsRequest = new NodesHotThreadsRequest(nodesIds);
        nodesHotThreadsRequest.threads(request.paramAsInt("threads", nodesHotThreadsRequest.threads()));
        nodesHotThreadsRequest.ignoreIdleThreads(request.paramAsBoolean("ignore_idle_threads", nodesHotThreadsRequest.ignoreIdleThreads()));
        nodesHotThreadsRequest.type(request.param("type", nodesHotThreadsRequest.type()));
        nodesHotThreadsRequest.interval(TimeValue.parseTimeValue(request.param("interval"), nodesHotThreadsRequest.interval(), "interval"));
        nodesHotThreadsRequest.snapshots(request.paramAsInt("snapshots", nodesHotThreadsRequest.snapshots()));
        nodesHotThreadsRequest.timeout(request.param("timeout"));
        return channel -> client.admin().cluster().nodesHotThreads(
                nodesHotThreadsRequest,
                new RestResponseListener<NodesHotThreadsResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(NodesHotThreadsResponse response) throws Exception {
                        StringBuilder sb = new StringBuilder();
                        for (NodeHotThreads node : response.getNodes()) {
                            sb.append("::: ").append(node.getNode().toString()).append("\n");
                            Strings.spaceify(3, node.getHotThreads(), sb);
                            sb.append('\n');
                        }
                        return new BytesRestResponse(RestStatus.OK, sb.toString());
                    }
                });
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
