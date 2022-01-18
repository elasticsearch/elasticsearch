/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.IncompatibleClusterStateVersionException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.PositionTrackingOutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class PublicationTransportHandler {

    private static final Logger logger = LogManager.getLogger(PublicationTransportHandler.class);

    public static final String PUBLISH_STATE_ACTION_NAME = "internal:cluster/coordination/publish_state";
    public static final String COMMIT_STATE_ACTION_NAME = "internal:cluster/coordination/commit_state";

    private final TransportService transportService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest;

    private final AtomicReference<ClusterState> lastSeenClusterState = new AtomicReference<>();

    /**
     * Keeps hold of any ongoing publication to avoid having to de/serialize the whole thing on the master too; instead we send a small
     * placeholder through the transport service and use the request recorded here.
     */
    private final AtomicReference<PublishRequest> currentPublishRequestToSelf = new AtomicReference<>();

    private final AtomicLong fullClusterStateReceivedCount = new AtomicLong();
    private final AtomicLong incompatibleClusterStateDiffReceivedCount = new AtomicLong();
    private final AtomicLong compatibleClusterStateDiffReceivedCount = new AtomicLong();
    // -> no need to put a timeout on the options here, because we want the response to eventually be received
    // and not log an error if it arrives after the timeout
    private static final TransportRequestOptions STATE_REQUEST_OPTIONS = TransportRequestOptions.of(
        null,
        TransportRequestOptions.Type.STATE
    );

    private final SerializationStatsTracker serializationStatsTracker = new SerializationStatsTracker();

    public PublicationTransportHandler(
        TransportService transportService,
        NamedWriteableRegistry namedWriteableRegistry,
        Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest,
        BiConsumer<ApplyCommitRequest, ActionListener<Void>> handleApplyCommit
    ) {
        this.transportService = transportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.handlePublishRequest = handlePublishRequest;

        transportService.registerRequestHandler(
            PUBLISH_STATE_ACTION_NAME,
            ThreadPool.Names.GENERIC,
            false,
            false,
            BytesTransportRequest::new,
            (request, channel, task) -> channel.sendResponse(handleIncomingPublishRequest(request))
        );

        transportService.registerRequestHandler(
            COMMIT_STATE_ACTION_NAME,
            ThreadPool.Names.GENERIC,
            false,
            false,
            ApplyCommitRequest::new,
            (request, channel, task) -> handleApplyCommit.accept(
                request,
                new ChannelActionListener<>(channel, COMMIT_STATE_ACTION_NAME, request).map(r -> TransportResponse.Empty.INSTANCE)
            )
        );
    }

    public PublishClusterStateStats stats() {
        return new PublishClusterStateStats(
            fullClusterStateReceivedCount.get(),
            incompatibleClusterStateDiffReceivedCount.get(),
            compatibleClusterStateDiffReceivedCount.get(),
            serializationStatsTracker.getSerializationStats()
        );
    }

    private PublishWithJoinResponse handleIncomingPublishRequest(BytesTransportRequest request) throws IOException {
        final Compressor compressor = CompressorFactory.compressor(request.bytes());
        StreamInput in = request.bytes().streamInput();
        try {
            if (compressor != null) {
                in = new InputStreamStreamInput(compressor.threadLocalInputStream(in));
            }
            in = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);
            in.setVersion(request.version());

            return switch (PublishRequestType.readFrom(in)) {
                case FULL -> {
                    final ClusterState incomingState;
                    // Close early to release resources used by the de-compression as early as possible
                    try (StreamInput input = in) {
                        incomingState = ClusterState.readFrom(input, transportService.getLocalNode());
                    } catch (Exception e) {
                        logger.warn("unexpected error while deserializing an incoming cluster state", e);
                        assert false : e;
                        throw e;
                    }
                    fullClusterStateReceivedCount.incrementAndGet();
                    logger.debug(
                        "received full cluster state version [{}] with size [{}]",
                        incomingState.version(),
                        request.bytes().length()
                    );
                    final PublishWithJoinResponse response = acceptState(incomingState);
                    lastSeenClusterState.set(incomingState);
                    yield response;
                }

                case DIFF -> {
                    final ClusterState lastSeen = lastSeenClusterState.get();
                    if (lastSeen == null) {
                        logger.debug("received diff for but don't have any local cluster state - requesting full state");
                        incompatibleClusterStateDiffReceivedCount.incrementAndGet();
                        throw new IncompatibleClusterStateVersionException("have no local cluster state");
                    }

                    ClusterState incomingState;
                    try {
                        final Diff<ClusterState> diff;
                        // Close stream early to release resources used by the de-compression as early as possible
                        try (StreamInput input = in) {
                            diff = ClusterState.readDiffFrom(input, lastSeen.nodes().getLocalNode());
                        }
                        incomingState = diff.apply(lastSeen); // might throw IncompatibleClusterStateVersionException
                    } catch (IncompatibleClusterStateVersionException e) {
                        incompatibleClusterStateDiffReceivedCount.incrementAndGet();
                        throw e;
                    } catch (Exception e) {
                        logger.warn("unexpected error while deserializing an incoming cluster state", e);
                        assert false : e;
                        throw e;
                    }
                    compatibleClusterStateDiffReceivedCount.incrementAndGet();
                    logger.debug(
                        "received diff cluster state version [{}] with uuid [{}], diff size [{}]",
                        incomingState.version(),
                        incomingState.stateUUID(),
                        request.bytes().length()
                    );
                    final PublishWithJoinResponse response = acceptState(incomingState);
                    lastSeenClusterState.compareAndSet(lastSeen, incomingState);
                    yield response;
                }

                case LOCAL -> {
                    final String incomingUUID = in.readString();
                    final PublishRequest publishRequest = currentPublishRequestToSelf.get();
                    if (publishRequest == null || publishRequest.getAcceptedState().stateUUID().equals(incomingUUID) == false) {
                        throw new IllegalStateException("publication to self failed for " + publishRequest);
                    }
                    final PublishWithJoinResponse response = handlePublishRequest.apply(publishRequest);
                    lastSeenClusterState.set(publishRequest.getAcceptedState());
                    yield response;
                }
            };
        } finally {
            IOUtils.close(in);
        }
    }

    private PublishWithJoinResponse acceptState(ClusterState incomingState) {
        assert incomingState.nodes().isLocalNodeElectedMaster() == false
            : "should handle local publications locally, but got " + incomingState;
        return handlePublishRequest.apply(new PublishRequest(incomingState));
    }

    public PublicationContext newPublicationContext(ClusterStatePublicationEvent clusterStatePublicationEvent) {
        final PublicationContext publicationContext = new PublicationContext(clusterStatePublicationEvent);
        boolean success = false;
        try {
            // Build the serializations we expect to need now, early in the process, so that an error during serialization fails the
            // publication straight away. This isn't watertight since we send diffs on a best-effort basis and may fall back to sending a
            // full state (and therefore serializing it) if the diff-based publication fails.
            publicationContext.buildDiffAndSerializeStates();
            success = true;
            return publicationContext;
        } finally {
            if (success == false) {
                publicationContext.decRef();
            }
        }
    }

    private ReleasableBytesReference serializeFullClusterState(ClusterState clusterState, DiscoveryNode node) {
        final Version nodeVersion = node.getVersion();
        final RecyclerBytesStreamOutput bytesStream = transportService.newNetworkBytesStream();
        boolean success = false;
        try {
            final long uncompressedBytes;
            try (
                StreamOutput stream = new PositionTrackingOutputStreamStreamOutput(
                    CompressorFactory.COMPRESSOR.threadLocalOutputStream(Streams.flushOnCloseStream(bytesStream))
                )
            ) {
                stream.setVersion(nodeVersion);
                PublishRequestType.FULL.writeTo(stream);
                clusterState.writeTo(stream);
                uncompressedBytes = stream.position();
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize cluster state for publishing to node {}", e, node);
            }
            final ReleasableBytesReference result = new ReleasableBytesReference(bytesStream.bytes(), bytesStream);
            serializationStatsTracker.serializedFullState(uncompressedBytes, result.length());
            logger.trace(
                "serialized full cluster state version [{}] for node version [{}] with size [{}]",
                clusterState.version(),
                nodeVersion,
                result.length()
            );
            success = true;
            return result;
        } finally {
            if (success == false) {
                bytesStream.close();
            }
        }
    }

    private ReleasableBytesReference serializeDiffClusterState(long clusterStateVersion, Diff<ClusterState> diff, DiscoveryNode node) {
        final Version nodeVersion = node.getVersion();
        final RecyclerBytesStreamOutput bytesStream = transportService.newNetworkBytesStream();
        boolean success = false;
        try {
            final long uncompressedBytes;
            try (
                StreamOutput stream = new PositionTrackingOutputStreamStreamOutput(
                    CompressorFactory.COMPRESSOR.threadLocalOutputStream(Streams.flushOnCloseStream(bytesStream))
                )
            ) {
                stream.setVersion(nodeVersion);
                PublishRequestType.DIFF.writeTo(stream);
                diff.writeTo(stream);
                uncompressedBytes = stream.position();
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize cluster state diff for publishing to node {}", e, node);
            }
            final ReleasableBytesReference result = new ReleasableBytesReference(bytesStream.bytes(), bytesStream);
            serializationStatsTracker.serializedDiff(uncompressedBytes, result.length());
            logger.trace(
                "serialized cluster state diff for version [{}] for node version [{}] with size [{}]",
                clusterStateVersion,
                nodeVersion,
                result.length()
            );
            success = true;
            return result;
        } finally {
            if (success == false) {
                bytesStream.close();
            }
        }
    }

    /**
     * Publishing a cluster state typically involves sending the same cluster state (or diff) to every node, so the work of diffing,
     * serializing, and compressing the state can be done once and the results shared across publish requests. The
     * {@code PublicationContext} implements this sharing. It's ref-counted: the initial reference is released by the coordinator when
     * a state (or diff) has been sent to every node, every transmitted diff also holds a reference in case it needs to retry with a full
     * state.
     */
    public class PublicationContext extends AbstractRefCounted {

        private final DiscoveryNodes discoveryNodes;
        private final ClusterState newState;
        private final ClusterState previousState;
        private final boolean sendFullVersion;

        // All the values of these maps have one ref for the context (while it's open) and one for each in-flight message.
        private final Map<Version, ReleasableBytesReference> serializedStates = new ConcurrentHashMap<>();
        private final Map<Version, ReleasableBytesReference> serializedDiffs = new HashMap<>();

        PublicationContext(ClusterStatePublicationEvent clusterStatePublicationEvent) {
            discoveryNodes = clusterStatePublicationEvent.getNewState().nodes();
            newState = clusterStatePublicationEvent.getNewState();
            previousState = clusterStatePublicationEvent.getOldState();
            sendFullVersion = previousState.getBlocks().disableStatePersistence();
        }

        void buildDiffAndSerializeStates() {
            assert refCount() > 0;
            final LazyInitializable<Diff<ClusterState>, RuntimeException> diffSupplier = new LazyInitializable<>(
                () -> newState.diff(previousState)
            );
            for (DiscoveryNode node : discoveryNodes) {
                if (node.equals(transportService.getLocalNode())) {
                    // publication to local node bypasses any serialization
                    continue;
                }
                if (sendFullVersion || previousState.nodes().nodeExists(node) == false) {
                    serializedStates.computeIfAbsent(node.getVersion(), v -> serializeFullClusterState(newState, node));
                } else {
                    serializedDiffs.computeIfAbsent(
                        node.getVersion(),
                        v -> serializeDiffClusterState(newState.version(), diffSupplier.getOrCompute(), node)
                    );
                }
            }
        }

        public void sendPublishRequest(
            DiscoveryNode destination,
            PublishRequest publishRequest,
            ActionListener<PublishWithJoinResponse> listener
        ) {
            assert refCount() > 0;
            assert publishRequest.getAcceptedState() == newState : "state got switched on us";
            assert transportService.getThreadPool().getThreadContext().isSystemContext();
            if (destination.equals(discoveryNodes.getLocalNode())) {

                // The master needs the original non-serialized state as the cluster state contains some volatile information that we
                // don't want to be replicated because it's not usable on another node (e.g. UnassignedInfo.unassignedTimeNanos) or
                // because it's mostly just debugging info that would unnecessarily blow up CS updates (I think there was one in
                // snapshot code). We may be able to remove this in future.
                //
                // Also, the transport service normally avoids serializing/deserializing requests to the local node but here we have special
                // handling to share the serialized representation of the cluster state across requests, so we must also handle local
                // requests differently to avoid having to decompress and deserialize the request on the master.

                logger.trace("handling cluster state version [{}] locally on [{}]", newState.version(), destination);

                final RecyclerBytesStreamOutput bytesStream = transportService.newNetworkBytesStream();
                boolean success = false;
                try {
                    try (StreamOutput stream = new OutputStreamStreamOutput(Streams.flushOnCloseStream(bytesStream))) {
                        stream.setVersion(Version.CURRENT);
                        PublishRequestType.LOCAL.writeTo(stream);
                        stream.writeString(newState.stateUUID());
                    } catch (IOException e) {
                        listener.onFailure(
                            new ElasticsearchException("failed to serialize cluster state for publishing to local node {}", e, destination)
                        );
                        return;
                    }

                    final ReleasableBytesReference result = new ReleasableBytesReference(bytesStream.bytes(), bytesStream);

                    final PublishRequest previousRequest = currentPublishRequestToSelf.getAndSet(publishRequest);
                    assert previousRequest == null || previousRequest.getAcceptedState().term() < publishRequest.getAcceptedState().term();

                    sendClusterState(destination, result, new ActionListener<>() {
                        @Override
                        public void onResponse(PublishWithJoinResponse publishWithJoinResponse) {
                            currentPublishRequestToSelf.compareAndSet(publishRequest, null); // only clean-up our mess
                            listener.onResponse(publishWithJoinResponse);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            currentPublishRequestToSelf.compareAndSet(publishRequest, null); // only clean-up our mess
                            listener.onFailure(e);
                        }
                    });

                    success = true;
                } finally {
                    if (success == false) {
                        bytesStream.close();
                    }
                }
            } else if (sendFullVersion || previousState.nodes().nodeExists(destination) == false) {
                logger.trace("sending full cluster state version [{}] to [{}]", newState.version(), destination);
                sendFullClusterState(destination, listener);
            } else {
                logger.trace("sending cluster state diff for version [{}] to [{}]", newState.version(), destination);
                sendClusterStateDiff(destination, listener);
            }
        }

        public void sendApplyCommit(
            DiscoveryNode destination,
            ApplyCommitRequest applyCommitRequest,
            ActionListener<TransportResponse.Empty> listener
        ) {
            assert transportService.getThreadPool().getThreadContext().isSystemContext();
            transportService.sendRequest(
                destination,
                COMMIT_STATE_ACTION_NAME,
                applyCommitRequest,
                STATE_REQUEST_OPTIONS,
                new ActionListenerResponseHandler<>(listener, in -> TransportResponse.Empty.INSTANCE, ThreadPool.Names.GENERIC)
            );
        }

        private void sendFullClusterState(DiscoveryNode destination, ActionListener<PublishWithJoinResponse> listener) {
            assert refCount() > 0;
            ReleasableBytesReference bytes = serializedStates.get(destination.getVersion());
            if (bytes == null) {
                try {
                    bytes = serializedStates.computeIfAbsent(
                        destination.getVersion(),
                        v -> serializeFullClusterState(newState, destination)
                    );
                } catch (Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage("failed to serialize cluster state before publishing it to node {}", destination),
                        e
                    );
                    listener.onFailure(e);
                    return;
                }
            }
            sendClusterState(destination, bytes, listener);
        }

        private void sendClusterStateDiff(DiscoveryNode destination, ActionListener<PublishWithJoinResponse> listener) {
            final ReleasableBytesReference bytes = serializedDiffs.get(destination.getVersion());
            assert bytes != null
                : "failed to find serialized diff for node " + destination + " of version [" + destination.getVersion() + "]";

            // acquire a ref to the context just in case we need to try again with the full cluster state
            if (tryIncRef() == false) {
                assert false;
                listener.onFailure(new IllegalStateException("publication context released before transmission"));
                return;
            }
            sendClusterState(destination, bytes, ActionListener.runAfter(listener.delegateResponse((delegate, e) -> {
                if (e instanceof final TransportException transportException) {
                    if (transportException.unwrapCause() instanceof IncompatibleClusterStateVersionException) {
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "resending full cluster state to node {} reason {}",
                                destination,
                                transportException.getDetailedMessage()
                            )
                        );
                        sendFullClusterState(destination, delegate);
                        return;
                    }
                }

                logger.debug(new ParameterizedMessage("failed to send cluster state to {}", destination), e);
                delegate.onFailure(e);
            }), this::decRef));
        }

        private void sendClusterState(
            DiscoveryNode destination,
            ReleasableBytesReference bytes,
            ActionListener<PublishWithJoinResponse> listener
        ) {
            assert refCount() > 0;
            if (bytes.tryIncRef() == false) {
                assert false;
                listener.onFailure(new IllegalStateException("serialized cluster state released before transmission"));
                return;
            }
            try {
                transportService.sendRequest(
                    destination,
                    PUBLISH_STATE_ACTION_NAME,
                    new BytesTransportRequest(bytes, destination.getVersion()),
                    STATE_REQUEST_OPTIONS,
                    new ActionListenerResponseHandler<PublishWithJoinResponse>(
                        ActionListener.runAfter(listener, bytes::decRef),
                        PublishWithJoinResponse::new,
                        ThreadPool.Names.GENERIC
                    )
                );
            } catch (Exception e) {
                assert false : e;
                logger.warn(() -> new ParameterizedMessage("error sending cluster state to {}", destination), e);
                listener.onFailure(e);
            }
        }

        @Override
        protected void closeInternal() {
            serializedDiffs.values().forEach(Releasables::closeExpectNoException);
            serializedStates.values().forEach(Releasables::closeExpectNoException);
        }
    }

    private static class SerializationStatsTracker {

        private long fullStateCount;
        private long totalUncompressedFullStateBytes;
        private long totalCompressedFullStateBytes;

        private long diffCount;
        private long totalUncompressedDiffBytes;
        private long totalCompressedDiffBytes;

        public synchronized void serializedFullState(long uncompressedBytes, int compressedBytes) {
            fullStateCount += 1;
            totalUncompressedFullStateBytes += uncompressedBytes;
            totalCompressedFullStateBytes += compressedBytes;
        }

        public synchronized void serializedDiff(long uncompressedBytes, int compressedBytes) {
            diffCount += 1;
            totalUncompressedDiffBytes += uncompressedBytes;
            totalCompressedDiffBytes += compressedBytes;
        }

        public synchronized ClusterStateSerializationStats getSerializationStats() {
            return new ClusterStateSerializationStats(
                fullStateCount,
                totalUncompressedFullStateBytes,
                totalCompressedFullStateBytes,
                diffCount,
                totalUncompressedDiffBytes,
                totalCompressedDiffBytes
            );
        }
    }

    private enum PublishRequestType implements Writeable {
        /**
         * The rest of the publish request is a diff between the previous and current cluster states.
         */
        DIFF(0),

        /**
         * The rest of the publish request is the current cluster state serialized in full.
         */
        FULL(1),

        /**
         * This publish request is being sent from the master to itself, and only contains the state UUID to validate that the in-memory
         * state is the right one.
         */
        LOCAL(2);

        private final byte b;

        PublishRequestType(int b) {
            this.b = (byte) b;
        }

        public static PublishRequestType readFrom(StreamInput in) throws IOException {
            final byte b = in.readByte();
            switch (b) {
                case 0:
                    return DIFF;
                case 1:
                    return FULL;
                case 2:
                    return LOCAL;
                default:
                    throw new IllegalStateException("unexpected PublishRequestType[" + b + "]");
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(b);
        }
    }

}
