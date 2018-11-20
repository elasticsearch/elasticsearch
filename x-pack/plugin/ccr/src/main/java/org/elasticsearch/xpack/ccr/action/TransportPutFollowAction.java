/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.respository.RemoteClusterRepository;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.io.IOException;
import java.util.Objects;

public final class TransportPutFollowAction
    extends TransportMasterNodeAction<PutFollowAction.Request, PutFollowAction.Response> {

    private final Client client;
    private final RestoreService restoreService;
    private final ActiveShardsObserver activeShardsObserver;
    private final CcrLicenseChecker ccrLicenseChecker;

    @Inject
    public TransportPutFollowAction(
        final ThreadPool threadPool,
        final TransportService transportService,
        final ClusterService clusterService,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Client client,
        final RestoreService restoreService,
        final CcrLicenseChecker ccrLicenseChecker) {
        super(
            PutFollowAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutFollowAction.Request::new,
            indexNameExpressionResolver);
        this.client = client;
        this.restoreService = restoreService;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutFollowAction.Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    protected PutFollowAction.Response read(StreamInput in) throws IOException {
        return new PutFollowAction.Response(in);
    }

    @Override
    protected void masterOperation(
        final PutFollowAction.Request request,
        final ClusterState state,
        final ActionListener<PutFollowAction.Response> listener) throws Exception {
        if (ccrLicenseChecker.isCcrAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("ccr"));
            return;
        }
        String remoteCluster = request.getRemoteCluster();
        // Validates whether the leader cluster has been configured properly:
        client.getRemoteClusterClient(remoteCluster);

        String leaderIndex = request.getLeaderIndex();
        ccrLicenseChecker.checkRemoteClusterLicenseAndFetchLeaderIndexMetadataAndHistoryUUIDs(
            client,
            remoteCluster,
            leaderIndex,
            listener::onFailure,
            (historyUUID, leaderIndexMetaData) -> createFollowerIndex(leaderIndexMetaData, historyUUID, request, listener));
    }

    private void createFollowerIndex(
        final IndexMetaData leaderIndexMetaData,
        final String[] historyUUIDs,
        final PutFollowAction.Request request,
        final ActionListener<PutFollowAction.Response> listener) {

        if (leaderIndexMetaData == null) {
            listener.onFailure(new IllegalArgumentException("leader index [" + request.getLeaderIndex() + "] does not exist"));
            return;
        }
        if (leaderIndexMetaData.getSettings().getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false) == false) {
            listener.onFailure(
                new IllegalArgumentException("leader index [" + request.getLeaderIndex() + "] does not have soft deletes enabled"));
            return;
        }

        String remoteCluster = request.getRemoteCluster();

        ActionListener<RestoreSnapshotResponse> restoreCompleteHandler = new ActionListener<RestoreSnapshotResponse>() {
            @Override
            public void onResponse(RestoreSnapshotResponse restoreSnapshotResponse) {
                RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
                if (restoreInfo.failedShards() == 0) {
                    initiateFollowing(request, listener);
                } else {
                    listener.onFailure(new ElasticsearchException("failed to restore [" + restoreInfo.failedShards() + "] shards"));
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);

            }
        };

        client.admin().cluster().preparePutRepository(remoteCluster).setType(RemoteClusterRepository.TYPE)
            .execute(new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    if (acknowledgedResponse.isAcknowledged()) {
                        Settings.Builder settingsBuilder = Settings.builder()
                            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
                            // TODO: Figure out what to do with private setting SETTING_INDEX_PROVIDED_NAME
                            .put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, request.getFollowRequest().getFollowerIndex())
                            // Overwriting UUID here, because otherwise we can't follow indices in the same cluster
                            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);
                        RestoreService.RestoreRequest restoreRequest = new RestoreService.RestoreRequest(remoteCluster,
                            leaderIndexMetaData.getIndex().getName(), new String[]{request.getLeaderIndex()}, request.indicesOptions(),
                            "^(.*)$", request.getFollowRequest().getFollowerIndex(), Settings.EMPTY, request.masterNodeTimeout(), false,
                            false, true, settingsBuilder.build(), new String[0], "restore_snapshot[" + remoteCluster + "]");
                        initiateRestore(restoreRequest, restoreCompleteHandler);
                    } else {
                        listener.onFailure(new ElasticsearchException("remote cluster repository put not acknowledged"));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    private void initiateRestore(RestoreService.RestoreRequest restoreRequest, ActionListener<RestoreSnapshotResponse> listener) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() ->
            restoreService.restoreSnapshot(restoreRequest, new ActionListener<RestoreService.RestoreCompletionResponse>() {
                @Override
                public void onResponse(RestoreService.RestoreCompletionResponse restoreCompletionResponse) {
                    final Snapshot snapshot = restoreCompletionResponse.getSnapshot();

                    ClusterStateListener clusterStateListener = new ClusterStateListener() {
                        @Override
                        public void clusterChanged(ClusterChangedEvent changedEvent) {
                            RestoreInProgress.Entry prevEntry = RestoreService.restoreInProgress(changedEvent.previousState(), snapshot);
                            RestoreInProgress.Entry newEntry = RestoreService.restoreInProgress(changedEvent.state(), snapshot);
                            if (prevEntry == null) {
                                // TODO: Does this block still make sense?
                                // When there is a master failure after a restore has been started, this listener might not be registered
                                // on the current master and as such it might miss some intermediary cluster states due to batching.
                                // Clean up listener in that case and acknowledge completion of restore operation to client.
                                clusterService.removeListener(this);
                                listener.onResponse(null);
                                tearDownRepository();
                            } else if (newEntry == null) {
                                clusterService.removeListener(this);
                                ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards = prevEntry.shards();
                                assert prevEntry.state().completed() : "expected completed snapshot state but was " + prevEntry.state();
                                assert RestoreService.completed(shards) : "expected all restore entries to be completed";
                                RestoreInfo restoreInfo = new RestoreInfo(prevEntry.snapshot().getSnapshotId().getName(),
                                    prevEntry.indices(), shards.size(), shards.size() - RestoreService.failedShards(shards));
                                RestoreSnapshotResponse response = new RestoreSnapshotResponse(restoreInfo);
                                logger.debug("restore of [{}] completed", snapshot);
                                listener.onResponse(response);
                                tearDownRepository();
                            } else {
                                // restore not completed yet, wait for next cluster state update
                            }
                        }
                    };

                    clusterService.addListener(clusterStateListener);
                }

                @Override
                public void onFailure(Exception t) {
                    listener.onFailure(t);
                    tearDownRepository();
                }

                private void tearDownRepository() {
                    client.admin().cluster().prepareDeleteRepository(restoreRequest.repositoryName()).execute();
                }
            }, false, false));

    }

    private void initiateFollowing(
        final PutFollowAction.Request request,
        final ActionListener<PutFollowAction.Response> listener) {
        activeShardsObserver.waitForActiveShards(new String[]{request.getFollowRequest().getFollowerIndex()},
            ActiveShardCount.DEFAULT, request.timeout(), result -> {
                if (result) {
                    client.execute(ResumeFollowAction.INSTANCE, request.getFollowRequest(), ActionListener.wrap(
                        r -> listener.onResponse(new PutFollowAction.Response(true, true, r.isAcknowledged())),
                        listener::onFailure
                    ));
                } else {
                    listener.onResponse(new PutFollowAction.Response(true, false, false));
                }
            }, listener::onFailure);
    }

    @Override
    protected ClusterBlockException checkBlock(final PutFollowAction.Request request, final ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getFollowRequest().getFollowerIndex());
    }

}
