/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.state;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalClusterStateAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.Custom;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public class TransportClusterStateAction extends TransportLocalClusterStateAction<ClusterStateRequest, ClusterStateResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterStateAction.class);

    private final ThreadPool threadPool;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @SuppressWarnings("this-escape")
    @Inject
    public TransportClusterStateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterStateAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;

        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            true,
            ClusterStateRequest::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterStateRequest request, ClusterState state) {
        // cluster state calls are done also on a fully blocked cluster to figure out what is going
        // on in the cluster. For example, which nodes have joined yet the recovery has not yet kicked
        // in, we need to make sure we allow those calls
        // return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
        return null;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        final ClusterStateRequest request,
        final ClusterState state,
        final ActionListener<ClusterStateResponse> listener
    ) throws IOException {

        assert task instanceof CancellableTask : task + " not cancellable";
        final CancellableTask cancellableTask = (CancellableTask) task;

        final Predicate<ClusterState> acceptableClusterStatePredicate = request.waitForMetadataVersion() == null
            ? Predicates.always()
            : clusterState -> clusterState.metadata().version() >= request.waitForMetadataVersion();

        if (cancellableTask.notifyIfCancelled(listener)) {
            return;
        }
        if (acceptableClusterStatePredicate.test(state)) {
            ActionListener.completeWith(listener, () -> buildResponse(request, state));
        } else {
            assert acceptableClusterStatePredicate.test(state) == false;
            new ClusterStateObserver(state, clusterService, request.waitForTimeout(), logger, threadPool.getThreadContext())
                .waitForNextChange(new ClusterStateObserver.Listener() {

                    @Override
                    public void onNewClusterState(ClusterState newState) {
                        if (cancellableTask.notifyIfCancelled(listener)) {
                            return;
                        }

                        if (acceptableClusterStatePredicate.test(newState)) {
                            executor.execute(ActionRunnable.supply(listener, () -> buildResponse(request, newState)));
                        } else {
                            listener.onFailure(
                                new NotMasterException(
                                    "master stepped down waiting for metadata version " + request.waitForMetadataVersion()
                                )
                            );
                        }
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        ActionListener.run(listener, l -> {
                            if (cancellableTask.notifyIfCancelled(l) == false) {
                                l.onResponse(new ClusterStateResponse(state.getClusterName(), null, true));
                            }
                        });
                    }
                }, clusterState -> cancellableTask.isCancelled() || acceptableClusterStatePredicate.test(clusterState));
        }
    }

    @SuppressForbidden(reason = "exposing ClusterState#compatibilityVersions requires reading them")
    private static Map<String, CompatibilityVersions> getCompatibilityVersions(ClusterState clusterState) {
        return clusterState.compatibilityVersions();
    }

    @SuppressForbidden(reason = "exposing ClusterState#clusterFeatures requires reading them")
    private static Map<String, Set<String>> getClusterFeatures(ClusterState clusterState) {
        return clusterState.clusterFeatures().nodeFeatures();
    }

    private ClusterStateResponse buildResponse(final ClusterStateRequest request, final ClusterState currentState) {
        ThreadPool.assertCurrentThreadPool(ThreadPool.Names.MANAGEMENT); // too heavy to construct & serialize cluster state without forking

        if (request.blocks() == false) {
            final var blockException = currentState.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
            if (blockException != null) {
                // There's a METADATA_READ block in place, but we aren't returning it to the caller, and yet the caller needs to know that
                // this block exists (e.g. it's the STATE_NOT_RECOVERED_BLOCK, so the rest of the state is known to be incomplete). Thus we
                // must fail the request:
                throw blockException;
            }
        }

        logger.trace("Serving cluster state request using version {}", currentState.version());
        ClusterState.Builder builder = ClusterState.builder(currentState.getClusterName());
        builder.version(currentState.version());
        builder.stateUUID(currentState.stateUUID());

        if (request.nodes()) {
            builder.nodes(currentState.nodes());
            builder.nodeIdsToCompatibilityVersions(getCompatibilityVersions(currentState));
            builder.nodeFeatures(getClusterFeatures(currentState));
        }
        if (request.routingTable()) {
            if (request.indices().length > 0) {
                RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
                String[] indices = indexNameExpressionResolver.concreteIndexNames(currentState, request);
                for (String filteredIndex : indices) {
                    if (currentState.routingTable().getIndicesRouting().containsKey(filteredIndex)) {
                        routingTableBuilder.add(currentState.routingTable().getIndicesRouting().get(filteredIndex));
                    }
                }
                builder.routingTable(routingTableBuilder.build());
            } else {
                builder.routingTable(currentState.routingTable());
            }
        }
        if (request.blocks()) {
            builder.blocks(currentState.blocks());
        }

        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.clusterUUID(currentState.metadata().clusterUUID());
        mdBuilder.coordinationMetadata(currentState.coordinationMetadata());

        if (request.metadata()) {
            if (request.indices().length > 0) {
                mdBuilder.version(currentState.metadata().version());
                String[] indices = indexNameExpressionResolver.concreteIndexNames(currentState, request);
                for (String filteredIndex : indices) {
                    // If the requested index is part of a data stream then that data stream should also be included:
                    IndexAbstraction indexAbstraction = currentState.metadata().getIndicesLookup().get(filteredIndex);
                    if (indexAbstraction.getParentDataStream() != null) {
                        DataStream dataStream = indexAbstraction.getParentDataStream();
                        // Also the IMD of other backing indices need to be included, otherwise the cluster state api
                        // can't create a valid cluster state instance:
                        for (Index backingIndex : dataStream.getIndices()) {
                            mdBuilder.put(currentState.metadata().index(backingIndex), false);
                        }
                        mdBuilder.put(dataStream);
                    } else {
                        IndexMetadata indexMetadata = currentState.metadata().index(filteredIndex);
                        if (indexMetadata != null) {
                            mdBuilder.put(indexMetadata, false);
                        }
                    }
                }
            } else {
                mdBuilder = Metadata.builder(currentState.metadata());
            }

            // filter out metadata that shouldn't be returned by the API
            for (Map.Entry<String, Custom> custom : currentState.metadata().customs().entrySet()) {
                if (custom.getValue().context().contains(Metadata.XContentContext.API) == false) {
                    mdBuilder.removeCustom(custom.getKey());
                }
            }
        }
        builder.metadata(mdBuilder);

        if (request.customs()) {
            for (Map.Entry<String, ClusterState.Custom> custom : currentState.customs().entrySet()) {
                if (custom.getValue().isPrivate() == false) {
                    builder.putCustom(custom.getKey(), custom.getValue());
                }
            }
        }

        return new ClusterStateResponse(currentState.getClusterName(), builder.build(), false);
    }

}
