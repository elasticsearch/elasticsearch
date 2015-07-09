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
package org.elasticsearch.discovery.zen;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.membership.MembershipAction;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class processes incoming join request (passed zia {@link ZenDiscovery}). Incoming nodes
 * are directly added to the cluster state or are accumulated during master election.
 */
public class NodeJoinController extends AbstractComponent {

    final ClusterService clusterService;
    final RoutingService routingService;
    final DiscoverySettings discoverySettings;
    final AtomicBoolean accumulateJoins = new AtomicBoolean(false);

    // this is site while trying to become a master
    final AtomicReference<ElectionContext> electionContext = new AtomicReference<>();


    protected final Map<DiscoveryNode, List<MembershipAction.JoinCallback>> pendingJoinRequests = new HashMap<>();

    public NodeJoinController(ClusterService clusterService, RoutingService routingService, DiscoverySettings discoverySettings, Settings settings) {
        super(settings);
        this.clusterService = clusterService;
        this.routingService = routingService;
        this.discoverySettings = discoverySettings;
    }

    /**
     * waits for enough incoming joins from master eligible nodes to complete the master election
     * <p/>
     * You must start accumulating joins before calling this method. See {@link #startAccumulatingJoins()}
     * <p/>
     * The method will return once the local node has been elected as master or some failure/timeout has happened.
     * The exact outcome is communicated via the callback parameter, which is guaranteed to be called.
     *
     * @param requiredMasterJoins the number of joins from master eligible needed to complete the election
     * @param timeValue           how long to wait before failing. a timeout is communicated via the callback's onFailure method.
     * @param callback            the result of the election (success or failure) will be communicated by calling methods on this
     *                            object
     **/
    public void waitToBeElectedAsMaster(int requiredMasterJoins, TimeValue timeValue, final Callback callback) {
        assert accumulateJoins.get() : "waitToBeElectedAsMaster is called we are not accumulating joins";

        final CountDownLatch done = new CountDownLatch(1);
        final ElectionContext newContext = new ElectionContext(callback, requiredMasterJoins) {
            @Override
            void onClose() {
                if (electionContext.compareAndSet(this, null)) {
                    stopAccumulatingJoins();
                } else {
                    assert false : "failed to remove current election context";
                }
                done.countDown();
            }
        };

        if (electionContext.compareAndSet(null, newContext) == false) {
            // should never happen, but be conservative
            callback.onFailure(new IllegalStateException("double waiting for election"));
            return;
        }
        try {
            // check what we have so far..
            checkPendingJoinsAndElectIfNeeded();

            try {
                if (done.await(timeValue.millis(), TimeUnit.MILLISECONDS)) {
                    // callback handles everything
                    return;
                }
            } catch (InterruptedException e) {

            }
            if (logger.isTraceEnabled()) {
                final int pendingNodes;
                synchronized (pendingJoinRequests) {
                    pendingNodes = pendingJoinRequests.size();
                }
                logger.trace("timed out waiting to be elected. waited [{}]. pending node joins [{}]", timeValue, pendingNodes);
            }
            // callback will clear the context, if it's active
            newContext.onFailure(new ElasticsearchTimeoutException("timed out waiting to be elected"));
        } catch (Throwable t) {
            logger.error("unexpected failure while waiting for incoming joins", t);
            newContext.onFailure(t);
        }
    }

    public void startAccumulatingJoins() {
        logger.trace("starting to accumulate joins");
        boolean b = accumulateJoins.getAndSet(true);
        assert b == false : "double startAccumulatingJoins() calls";
        assert electionContext.get() == null : "startAccumulatingJoins() called, but there is an ongoing election context";
    }

    public void stopAccumulatingJoins() {
        logger.trace("stopping join accumulation");
        assert electionContext.get() == null : "stopAccumulatingJoins() called, but there is an ongoing election context";
        boolean b = accumulateJoins.getAndSet(false);
        assert b : "stopAccumulatingJoins() called but not accumulating";
        synchronized (pendingJoinRequests) {
            if (pendingJoinRequests.size() > 0) {
                processJoins("stopping to accumulate joins");
            }
        }
    }

    public void handleJoinRequest(final DiscoveryNode node, final MembershipAction.JoinCallback callback) {
        synchronized (pendingJoinRequests) {
            List<MembershipAction.JoinCallback> nodeCallbacks = pendingJoinRequests.get(node);
            if (nodeCallbacks == null) {
                nodeCallbacks = new ArrayList<>();
                pendingJoinRequests.put(node, nodeCallbacks);
            }
            nodeCallbacks.add(callback);
        }
        if (accumulateJoins.get() == false) {
            processJoins("join from node[" + node + "]");
        } else {
            checkPendingJoinsAndElectIfNeeded();
        }
    }

    private void checkPendingJoinsAndElectIfNeeded() {
        assert accumulateJoins.get() : "election check requested but we are not accumulating joins";
        final ElectionContext context = electionContext.get();
        if (context == null) {
            return;
        }

        int pendingMasterJoins=0;
        synchronized (pendingJoinRequests) {
            for (DiscoveryNode node : pendingJoinRequests.keySet()) {
                if (node.isMasterNode()) {
                    pendingMasterJoins++;
                }
            }
        }
        if (pendingMasterJoins < context.requiredMasterJoins) {
            logger.trace("not enough joins for election. Got [{}], required [{}]", pendingMasterJoins, context.requiredMasterJoins);
            return;
        }
        if (context.pendingSetAsMasterTask.getAndSet(true)) {
            logger.trace("elected as master task already submitted, ignoring...");
            return;
        }

        final String source = "zen-disco-join(elected_as_master, [" + pendingMasterJoins + "] joins received)";
        clusterService.submitStateUpdateTask(source, Priority.IMMEDIATE, new ProcessJoinsTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                // Take into account the previous known nodes, if they happen not to be available
                // then fault detection will remove these nodes.

                if (currentState.nodes().masterNode() != null) {
                    // TODO can we tie break here? we don't have a remote master cluster state version to decide on
                    logger.trace("join thread elected local node as master, but there is already a master in place: {}", currentState.nodes().masterNode());
                    throw new NotMasterException("Node [" + clusterService.localNode() + "] not master for join request");
                }

                DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder(currentState.nodes()).masterNodeId(currentState.nodes().localNode().id());
                // update the fact that we are the master...
                ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeGlobalBlock(discoverySettings.getNoMasterBlock()).build();
                currentState = ClusterState.builder(currentState).nodes(builder).blocks(clusterBlocks).build();

                // reroute now to remove any dead nodes (master may have stepped down when they left and didn't update the routing table)
                RoutingAllocation.Result result = routingService.getAllocationService().reroute(currentState);
                if (result.changed()) {
                    currentState = ClusterState.builder(currentState).routingResult(result).build();
                }

                // add the incoming join requests
                return super.execute(currentState);
            }

            @Override
            public boolean runOnlyOnMaster() {
                return false;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                super.onFailure(source, t);
                context.onFailure(t);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                super.clusterStateProcessed(source, oldState, newState);
                context.onElectedAsMaster(newState);
            }
        });
    }

    private void processJoins(String reason) {
        clusterService.submitStateUpdateTask("zen-disco-join(" + reason + ")", Priority.URGENT, new ProcessJoinsTask());
    }


    public interface Callback {
        void onElectedAsMaster(ClusterState state);

        void onFailure(Throwable t);
    }

    static abstract class ElectionContext implements Callback {
        private final Callback callback;
        private final int requiredMasterJoins;

        /** set to true after enough joins have been seen and a cluster update task is submitted to become master */
        final AtomicBoolean pendingSetAsMasterTask = new AtomicBoolean();
        final AtomicBoolean closed = new AtomicBoolean();

        ElectionContext(Callback callback, int requiredMasterJoins) {
            this.callback = callback;
            this.requiredMasterJoins = requiredMasterJoins;
        }

        abstract void onClose();

        @Override
        public void onElectedAsMaster(ClusterState state) {
            assert pendingSetAsMasterTask.get() : "onElectedAsMaster called but pendingSetAsMasterTask is not set";
            if (closed.compareAndSet(false, true)) {
                try {
                    onClose();
                } finally {
                    callback.onElectedAsMaster(state);
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            if (closed.compareAndSet(false, true)) {
                try {
                    onClose();
                } finally {
                    callback.onFailure(t);
                }
            }
        }
    }


    class ProcessJoinsTask extends ProcessedClusterStateUpdateTask {

        private final List<MembershipAction.JoinCallback> joinCallbacksToRespondTo = new ArrayList<>();
        private boolean nodeAdded = false;

        @Override
        public ClusterState execute(ClusterState currentState) {
            DiscoveryNodes.Builder nodesBuilder;
            synchronized (pendingJoinRequests) {
                if (pendingJoinRequests.isEmpty()) {
                    return currentState;
                }

                nodesBuilder = DiscoveryNodes.builder(currentState.nodes());
                Iterator<Map.Entry<DiscoveryNode, List<MembershipAction.JoinCallback>>> iterator = pendingJoinRequests.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<DiscoveryNode, List<MembershipAction.JoinCallback>> entry = iterator.next();
                    final DiscoveryNode node = entry.getKey();
                    joinCallbacksToRespondTo.addAll(entry.getValue());
                    iterator.remove();
                    if (currentState.nodes().nodeExists(node.id())) {
                        logger.debug("received a join request for an existing node [{}]", node);
                    } else {
                        nodeAdded = true;
                        nodesBuilder.put(node);
                        for (DiscoveryNode existingNode : currentState.nodes()) {
                            if (node.address().equals(existingNode.address())) {
                                nodesBuilder.remove(existingNode.id());
                                logger.warn("received join request from node [{}], but found existing node {} with same address, removing existing node", node, existingNode);
                            }
                        }
                    }
                }
            }

            // we must return a new cluster state instance to force publishing. This is important
            // for the joining node to finalize it's join and set us as a master
            final ClusterState.Builder newState = ClusterState.builder(currentState);
            if (nodeAdded) {
                newState.nodes(nodesBuilder);
            }

            return newState.build();
        }

        @Override
        public void onNoLongerMaster(String source) {
            // we are rejected, so drain all pending task (execute never run)
            synchronized (pendingJoinRequests) {
                Iterator<Map.Entry<DiscoveryNode, List<MembershipAction.JoinCallback>>> iterator = pendingJoinRequests.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<DiscoveryNode, List<MembershipAction.JoinCallback>> entry = iterator.next();
                    joinCallbacksToRespondTo.addAll(entry.getValue());
                    iterator.remove();
                }
            }
            Exception e = new NotMasterException("Node [" + clusterService.localNode() + "] not master for join request");
            innerOnFailure(e);
        }

        void innerOnFailure(Throwable t) {
            for (MembershipAction.JoinCallback callback : joinCallbacksToRespondTo) {
                try {
                    callback.onFailure(t);
                } catch (Exception e) {
                    logger.error("error during task failure", e);
                }
            }
        }

        @Override
        public void onFailure(String source, Throwable t) {
            logger.error("unexpected failure during [{}]", t, source);
            innerOnFailure(t);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            if (nodeAdded) {
                // we reroute not in the same cluster state update since in certain areas we rely on
                // the node to be in the cluster state (sampled from ClusterService#state) to be there, also
                // shard transitions need to better be handled in such cases
                routingService.reroute("post_node_add");
            }
            for (MembershipAction.JoinCallback callback : joinCallbacksToRespondTo) {
                try {
                    callback.onSuccess();
                } catch (Exception e) {
                    logger.error("unexpected error during [{}]", e, source);
                }
            }
        }
    }
}