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

package org.elasticsearch.action.replicatedrefresh;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class TransportReplicatedRefreshAction extends TransportReplicationAction<ReplicatedRefreshRequest, ReplicatedRefreshRequest, ReplicatedRefreshResponse> {

    private final ClusterService clusterService;

    @Inject
    public TransportReplicatedRefreshAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                            IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                            MappingUpdatedAction mappingUpdatedAction,
                                            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ReplicatedRefreshAction.NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, mappingUpdatedAction,
                actionFilters, indexNameExpressionResolver, ReplicatedRefreshRequest.class, ReplicatedRefreshRequest.class, ThreadPool.Names.REFRESH);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(final ReplicatedRefreshRequest request, final ActionListener<ReplicatedRefreshResponse> listener) {
        super.doExecute(request, listener);
    }

    @Override
    protected boolean resolveIndex() {
        return true;
    }

    @Override
    protected boolean checkWriteConsistency() {
        return false;
    }

    @Override
    protected ReplicatedRefreshResponse newResponseInstance() {
        return new ReplicatedRefreshResponse();
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, InternalRequest request) {
        return clusterService.operationRouting().shards(clusterService.state(), request.concreteIndex(), request.request().getShardId().id()).shardsIt();
    }

    @Override
    protected Tuple<ReplicatedRefreshResponse, ReplicatedRefreshRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) throws Throwable {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.shardId.getIndex()).shardSafe(shardRequest.shardId.id());
        indexShard.refresh("api");
        logger.trace("{} refresh request executed on primary", indexShard.shardId());
        int totalNumShards = clusterState.getMetaData().index(indexShard.shardId().index().getName()).getNumberOfReplicas() + 1;
        return new Tuple<>(new ReplicatedRefreshResponse(shardRequest.shardId, totalNumShards), shardRequest.request);
    }

    @Override
    protected void shardOperationOnReplica(ShardId shardId, ReplicatedRefreshRequest request) {
        IndexShard indexShard = indicesService.indexServiceSafe(request.getShardId().getIndex()).shardSafe(request.getShardId().id());
        indexShard.refresh("api");
        logger.trace("{} refresh request executed on replica", indexShard.shardId());
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, new String[]{request.concreteIndex()});
    }
}
