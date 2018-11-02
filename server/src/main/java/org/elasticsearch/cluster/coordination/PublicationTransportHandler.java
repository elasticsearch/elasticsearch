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
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.IncompatibleClusterStateVersionException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.discovery.zen.PublishClusterStateStats;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class PublicationTransportHandler extends AbstractComponent {

    public static final String PUBLISH_STATE_ACTION_NAME = "internal:cluster/coordination/publish_state";
    public static final String COMMIT_STATE_ACTION_NAME = "internal:cluster/coordination/commit_state";

    private final TransportService transportService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest;

    private AtomicReference<ClusterState> lastSeenClusterState = new AtomicReference<>();

    private final AtomicLong fullClusterStateReceivedCount = new AtomicLong();
    private final AtomicLong incompatibleClusterStateDiffReceivedCount = new AtomicLong();
    private final AtomicLong compatibleClusterStateDiffReceivedCount = new AtomicLong();

    public PublicationTransportHandler(Settings settings, TransportService transportService, NamedWriteableRegistry namedWriteableRegistry,
                                       Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest,
                                       BiConsumer<ApplyCommitRequest, ActionListener<Void>> handleApplyCommit) {
        super(settings);
        this.transportService = transportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.handlePublishRequest = handlePublishRequest;

        transportService.registerRequestHandler(PUBLISH_STATE_ACTION_NAME, BytesTransportRequest::new, ThreadPool.Names.GENERIC,
            false, false, (request, channel, task) -> handleIncomingPublishRequest(request, channel));

        transportService.registerRequestHandler(COMMIT_STATE_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            ApplyCommitRequest::new,
            (request, channel, task) -> handleApplyCommit.accept(request, new ActionListener<Void>() {

                @Override
                public void onResponse(Void aVoid) {
                    try {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    } catch (IOException e) {
                        logger.debug("failed to send response on commit", e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (IOException ie) {
                        e.addSuppressed(ie);
                        logger.debug("failed to send response on commit", e);
                    }
                }
            }));
    }

    public PublishClusterStateStats stats() {
        return new PublishClusterStateStats(
            fullClusterStateReceivedCount.get(),
            incompatibleClusterStateDiffReceivedCount.get(),
            compatibleClusterStateDiffReceivedCount.get());
    }

    public interface PublicationContext {

        void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                ActionListener<PublishWithJoinResponse> responseActionListener);

    }

    public PublicationContext newPublicationContext(ClusterChangedEvent clusterChangedEvent) {
        final DiscoveryNodes nodes = clusterChangedEvent.state().nodes();
        final ClusterState newState = clusterChangedEvent.state();
        final ClusterState previousState = clusterChangedEvent.previousState();
        final boolean sendFullVersion = clusterChangedEvent.previousState().getBlocks().disableStatePersistence();
        final Map<Version, BytesReference> serializedStates = new HashMap<>();
        final Map<Version, BytesReference> serializedDiffs = new HashMap<>();

        // we build these early as a best effort not to commit in the case of error.
        // sadly this is not water tight as it may that a failed diff based publishing to a node
        // will cause a full serialization based on an older version, which may fail after the
        // change has been committed.
        buildDiffAndSerializeStates(clusterChangedEvent.state(), clusterChangedEvent.previousState(),
            nodes, sendFullVersion, serializedStates, serializedDiffs);

        return (destination, publishRequest, responseActionListener) -> {
            assert publishRequest.getAcceptedState() == clusterChangedEvent.state() : "state got switched on us";
            if (destination.equals(nodes.getLocalNode())) {
                // the master needs the original non-serialized state as the cluster state contains some volatile information that we don't
                // want to be replicated because it's not usable on another node (e.g. UnassignedInfo.unassignedTimeNanos) or because it's
                // mostly just debugging info that would unnecessarily blow up CS updates (I think there was one in snapshot code).
                // TODO: look into these and check how to get rid of them
                transportService.getThreadPool().generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        // wrap into fake TransportException, as that's what we expect in Publication
                        responseActionListener.onFailure(new TransportException(e));
                    }

                    @Override
                    protected void doRun() {
                        responseActionListener.onResponse(handlePublishRequest.apply(publishRequest));
                    }

                    @Override
                    public String toString() {
                        return "publish to self of " + publishRequest;
                    }
                });
            } else if (sendFullVersion || !previousState.nodes().nodeExists(destination)) {
                sendFullClusterState(newState, serializedStates, destination, responseActionListener);
            } else {
                sendClusterStateDiff(newState, serializedDiffs, serializedStates, destination, responseActionListener);
            }
        };
    }

    private void sendClusterStateToNode(ClusterState clusterState, BytesReference bytes, DiscoveryNode node,
                                        ActionListener<PublishWithJoinResponse> responseActionListener, boolean sendDiffs,
                                        Map<Version, BytesReference> serializedStates) {
        try {
            // -> no need to put a timeout on the options here, because we want the response to eventually be received
            //  and not log an error if it arrives after the timeout
            // -> no need to compress, we already compressed the bytes
            TransportRequestOptions options = TransportRequestOptions.builder()
                .withType(TransportRequestOptions.Type.STATE).withCompress(false).build();
            transportService.sendRequest(node, PUBLISH_STATE_ACTION_NAME,
                new BytesTransportRequest(bytes, node.getVersion()),
                options,
                new TransportResponseHandler<PublishWithJoinResponse>() {

                    @Override
                    public PublishWithJoinResponse read(StreamInput in) throws IOException {
                        return new PublishWithJoinResponse(in);
                    }

                    @Override
                    public void handleResponse(PublishWithJoinResponse response) {
                        responseActionListener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        if (sendDiffs && exp.unwrapCause() instanceof IncompatibleClusterStateVersionException) {
                            logger.debug("resending full cluster state to node {} reason {}", node, exp.getDetailedMessage());
                            sendFullClusterState(clusterState, serializedStates, node, responseActionListener);
                        } else {
                            logger.debug(() -> new ParameterizedMessage("failed to send cluster state to {}", node), exp);
                            responseActionListener.onFailure(exp);
                        }
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }
                });
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("error sending cluster state to {}", node), e);
            responseActionListener.onFailure(e);
        }
    }

    private static void buildDiffAndSerializeStates(ClusterState clusterState, ClusterState previousState, DiscoveryNodes discoveryNodes,
                                                    boolean sendFullVersion, Map<Version, BytesReference> serializedStates,
                                                    Map<Version, BytesReference> serializedDiffs) {
        Diff<ClusterState> diff = null;
        for (DiscoveryNode node : discoveryNodes) {
            if (node.equals(discoveryNodes.getLocalNode())) {
                // ignore, see newPublicationContext
                continue;
            }
            try {
                if (sendFullVersion || !previousState.nodes().nodeExists(node)) {
                    serializedStates.putIfAbsent(node.getVersion(), serializeFullClusterState(clusterState, node.getVersion()));
                } else {
                    // will send a diff
                    if (diff == null) {
                        diff = clusterState.diff(previousState);
                    }
                    serializedDiffs.putIfAbsent(node.getVersion(), serializeDiffClusterState(diff, node.getVersion()));
                }
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize cluster state for publishing to node {}", e, node);
            }
        }
    }

    private void sendFullClusterState(ClusterState clusterState, Map<Version, BytesReference> serializedStates,
                                      DiscoveryNode node, ActionListener<PublishWithJoinResponse> responseActionListener) {
        BytesReference bytes = serializedStates.get(node.getVersion());
        if (bytes == null) {
            try {
                bytes = serializeFullClusterState(clusterState, node.getVersion());
                serializedStates.put(node.getVersion(), bytes);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to serialize cluster state before publishing it to node {}", node), e);
                responseActionListener.onFailure(e);
                return;
            }
        }
        sendClusterStateToNode(clusterState, bytes, node, responseActionListener, false, serializedStates);
    }

    private void sendClusterStateDiff(ClusterState clusterState,
                                      Map<Version, BytesReference> serializedDiffs, Map<Version, BytesReference> serializedStates,
                                      DiscoveryNode node, ActionListener<PublishWithJoinResponse> responseActionListener) {
        final BytesReference bytes = serializedDiffs.get(node.getVersion());
        assert bytes != null : "failed to find serialized diff for node " + node + " of version [" + node.getVersion() + "]";
        sendClusterStateToNode(clusterState, bytes, node, responseActionListener, true, serializedStates);
    }

    public static BytesReference serializeFullClusterState(ClusterState clusterState, Version nodeVersion) throws IOException {
        final BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = CompressorFactory.COMPRESSOR.streamOutput(bStream)) {
            stream.setVersion(nodeVersion);
            stream.writeBoolean(true);
            clusterState.writeTo(stream);
        }
        return bStream.bytes();
    }

    public static BytesReference serializeDiffClusterState(Diff diff, Version nodeVersion) throws IOException {
        final BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = CompressorFactory.COMPRESSOR.streamOutput(bStream)) {
            stream.setVersion(nodeVersion);
            stream.writeBoolean(false);
            diff.writeTo(stream);
        }
        return bStream.bytes();
    }

    private void handleIncomingPublishRequest(BytesTransportRequest request, TransportChannel channel) throws IOException {
        final Compressor compressor = CompressorFactory.compressor(request.bytes());
        StreamInput in = request.bytes().streamInput();
        final ClusterState incomingState;
        try {
            if (compressor != null) {
                in = compressor.streamInput(in);
            }
            in = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);
            in.setVersion(request.version());
            // If true we received full cluster state - otherwise diffs
            if (in.readBoolean()) {
                incomingState = ClusterState.readFrom(in, transportService.getLocalNode());
                fullClusterStateReceivedCount.incrementAndGet();
                logger.debug("received full cluster state version [{}] with size [{}]", incomingState.version(),
                    request.bytes().length());
                lastSeenClusterState.set(incomingState);
            } else {
                final ClusterState lastSeen = lastSeenClusterState.get();
                if (lastSeen == null) {
                    logger.debug("received diff for but don't have any local cluster state - requesting full state");
                    throw new IncompatibleClusterStateVersionException("have no local cluster state");
                } else {
                    Diff<ClusterState> diff = ClusterState.readDiffFrom(in, lastSeen.nodes().getLocalNode());
                    incomingState = diff.apply(lastSeen); // might throw IncompatibleClusterStateVersionException
                    compatibleClusterStateDiffReceivedCount.incrementAndGet();
                    logger.debug("received diff cluster state version [{}] with uuid [{}], diff size [{}]",
                        incomingState.version(), incomingState.stateUUID(), request.bytes().length());
                    lastSeenClusterState.compareAndSet(lastSeen, incomingState);
                }
            }
        } catch (IncompatibleClusterStateVersionException e) {
            incompatibleClusterStateDiffReceivedCount.incrementAndGet();
            throw e;
        } catch (Exception e) {
            logger.warn("unexpected error while deserializing an incoming cluster state", e);
            throw e;
        } finally {
            IOUtils.close(in);
        }

        channel.sendResponse(handlePublishRequest.apply(new PublishRequest(incomingState)));
    }

    public void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommitRequest,
                                ActionListener<TransportResponse.Empty> responseActionListener) {
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STATE).build();
        transportService.sendRequest(destination, COMMIT_STATE_ACTION_NAME, applyCommitRequest, options,
            new TransportResponseHandler<TransportResponse.Empty>() {

                @Override
                public TransportResponse.Empty read(StreamInput in) {
                    return TransportResponse.Empty.INSTANCE;
                }

                @Override
                public void handleResponse(TransportResponse.Empty response) {
                    responseActionListener.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    responseActionListener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }
            });
    }
}
