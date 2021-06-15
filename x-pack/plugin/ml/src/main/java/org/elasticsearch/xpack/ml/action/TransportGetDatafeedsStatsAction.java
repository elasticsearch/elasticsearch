/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedRunningStateAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction.Response;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction.Request;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TransportGetDatafeedsStatsAction extends TransportMasterNodeReadAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetDatafeedsStatsAction.class);

    private final DatafeedConfigProvider datafeedConfigProvider;
    private final JobResultsProvider jobResultsProvider;
    private final OriginSettingClient client;

    @Inject
    public TransportGetDatafeedsStatsAction(TransportService transportService, ClusterService clusterService,
                                            ThreadPool threadPool, ActionFilters actionFilters,
                                            IndexNameExpressionResolver indexNameExpressionResolver,
                                            DatafeedConfigProvider datafeedConfigProvider, JobResultsProvider jobResultsProvider,
                                            Client client) {
        super(
            GetDatafeedsStatsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            Response::new,
            ThreadPool.Names.SAME
        );
        this.datafeedConfigProvider = datafeedConfigProvider;
        this.jobResultsProvider = jobResultsProvider;
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    @Override
    protected void masterOperation(GetDatafeedsStatsAction.Request request, ClusterState state,
                                   ActionListener<GetDatafeedsStatsAction.Response> listener) throws Exception {
        logger.debug(() -> new ParameterizedMessage("[{}] get stats for datafeed", request.getDatafeedId()));
        final PersistentTasksCustomMetadata tasksInProgress = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        final Response.Builder responseBuilder = new Response.Builder();

        // 5. Build response
        ActionListener<GetDatafeedRunningStateAction.Response> runtimeStateListener = ActionListener.wrap(
            runtimeStateResponse -> {
                responseBuilder.setDatafeedRuntimeState(runtimeStateResponse);
                listener.onResponse(responseBuilder.build(tasksInProgress, state));
            },
            listener::onFailure
        );

        // 4. Grab runtime state
        ActionListener<Map<String, DatafeedTimingStats>> datafeedTimingStatsListener = ActionListener.wrap(
            timingStatsByJobId -> {
                responseBuilder.setTimingStatsMap(timingStatsByJobId);
                client.execute(
                    GetDatafeedRunningStateAction.INSTANCE,
                    new GetDatafeedRunningStateAction.Request(responseBuilder.getDatafeedIds()),
                    runtimeStateListener
                );
            },
            listener::onFailure
        );

        // 3. Grab timing stats
        ActionListener<List<DatafeedConfig.Builder>> expandedConfigsListener = ActionListener.wrap(
            datafeedBuilders -> {
                Map<String, String> datafeedIdsToJobIds = datafeedBuilders.stream()
                    .collect(Collectors.toMap(DatafeedConfig.Builder::getId, DatafeedConfig.Builder::getJobId));
                responseBuilder.setDatafeedToJobId(datafeedIdsToJobIds);
                jobResultsProvider.datafeedTimingStats(new ArrayList<>(datafeedIdsToJobIds.values()), datafeedTimingStatsListener);
            },
            listener::onFailure
        );

        // 2. Now that we have the ids, grab the datafeed configs
        ActionListener<SortedSet<String>> expandIdsListener = ActionListener.wrap(
            expandedIds -> {
                responseBuilder.setDatafeedIds(expandedIds);
                datafeedConfigProvider.expandDatafeedConfigs(
                    request.getDatafeedId(),
                    // Already took into account the request parameter when we expanded the IDs with the tasks earlier
                    // Should allow for no datafeeds in case the config is gone
                    true,
                    expandedConfigsListener
                );
            },
            listener::onFailure
        );

        // 1. This might also include datafeed tasks that exist but no longer have a config
        datafeedConfigProvider.expandDatafeedIds(request.getDatafeedId(),
            request.allowNoMatch(),
            tasksInProgress,
            true,
            expandIdsListener);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
