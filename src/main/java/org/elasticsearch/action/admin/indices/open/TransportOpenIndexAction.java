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

package org.elasticsearch.action.admin.indices.open;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportClusterStateUpdateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.action.support.master.ClusterStateUpdateActionListener;
import org.elasticsearch.action.support.master.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Open index action
 */
public class TransportOpenIndexAction extends TransportClusterStateUpdateAction<OpenIndexClusterStateUpdateRequest, ClusterStateUpdateResponse, OpenIndexRequest, OpenIndexResponse> {

    private final MetaDataIndexStateService indexStateService;

    @Inject
    public TransportOpenIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                    ThreadPool threadPool, MetaDataIndexStateService indexStateService) {
        super(settings, transportService, clusterService, threadPool);
        this.indexStateService = indexStateService;
    }

    @Override
    protected String executor() {
        // we go async right away...
        return ThreadPool.Names.SAME;
    }

    @Override
    protected String transportAction() {
        return OpenIndexAction.NAME;
    }

    @Override
    protected OpenIndexRequest newRequest() {
        return new OpenIndexRequest();
    }

    @Override
    protected OpenIndexResponse newResponse() {
        return new OpenIndexResponse();
    }

    @Override
    protected void doExecute(OpenIndexRequest request, ActionListener<OpenIndexResponse> listener) {
        request.indices(clusterService.state().metaData().concreteIndices(request.indices(), request.ignoreIndices(), false));
        super.doExecute(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(OpenIndexRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, request.indices());
    }

    @Override
    protected OpenIndexClusterStateUpdateRequest newClusterStateUpdateRequest(OpenIndexRequest acknowledgedRequest) {
        return new OpenIndexClusterStateUpdateRequest(acknowledgedRequest.indices());
    }

    @Override
    protected OpenIndexResponse newResponse(ClusterStateUpdateResponse updateResponse) {
        return new OpenIndexResponse(updateResponse.isAcknowledged());
    }

    @Override
    protected void updateClusterState(OpenIndexClusterStateUpdateRequest updateRequest, ClusterStateUpdateActionListener<ClusterStateUpdateResponse, OpenIndexResponse> listener) {
        indexStateService.openIndex(updateRequest, listener);
    }
}
