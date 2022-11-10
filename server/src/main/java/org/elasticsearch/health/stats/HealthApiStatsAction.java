/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.health.stats;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.metrics.Counters;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.List;

/**
 * This get the stats of the health API from every node
 */
public class HealthApiStatsAction extends ActionType<HealthApiStatsAction.Response> {

    public static final HealthApiStatsAction INSTANCE = new HealthApiStatsAction();
    public static final String NAME = "cluster:monitor/xpack/health/stats/dist";

    private HealthApiStatsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends BaseNodesRequest<Request> {

        public Request() {
            super((String[]) null);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public String toString() {
            return "health_api_stats";
        }

        public static class Node extends TransportRequest {

            public Node(StreamInput in) throws IOException {
                super(in);
            }

            public Node(Request request) {}

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
            }
        }
    }

    public static class Response extends BaseNodesResponse<Response.Node> {

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(ClusterName clusterName, List<Node> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        protected List<Node> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(Node::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<Node> nodes) throws IOException {
            out.writeList(nodes);
        }

        public static class Node extends BaseNodeResponse {
            private Counters stats;

            public Node(StreamInput in) throws IOException {
                super(in);
                if (in.readBoolean()) {
                    stats = new Counters(in);
                }
            }

            public Node(DiscoveryNode node) {
                super(node);
            }

            public Counters getStats() {
                return stats;
            }

            public void setStats(Counters stats) {
                this.stats = stats;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
            }
        }
    }
}
