/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;
import org.elasticsearch.xpack.ml.utils.TypedChainTaskExecutor;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ClientHelper.stashWithOrigin;
import static org.elasticsearch.xpack.core.ml.MlTasks.AWAITING_UPGRADE;
import static org.elasticsearch.xpack.core.ml.MlTasks.DATAFEED_TASK_NAME;
import static org.elasticsearch.xpack.core.ml.MlTasks.JOB_TASK_ID_PREFIX;

public class TransportSetUpgradeModeAction extends TransportMasterNodeAction<SetUpgradeModeAction.Request, AcknowledgedResponse> {

    private static final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final PersistentTasksClusterService persistentTasksClusterService;
    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public TransportSetUpgradeModeAction(TransportService transportService, ThreadPool threadPool, ClusterService clusterService,
                                         PersistentTasksClusterService persistentTasksClusterService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver, Client client) {
        super(SetUpgradeModeAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
            SetUpgradeModeAction.Request::new);
        this.persistentTasksClusterService = persistentTasksClusterService;
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(SetUpgradeModeAction.Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws Exception {

        if (request.isEnabled() == MlMetadata.getMlMetadata(state).isUpgradeMode()) {
            listener.onResponse(new AcknowledgedResponse(true));
            return;
        }

        // Switching the setting on/off while we are in the middle of adjusting task assignments and isolating datafeeds
        // may result in unknown behavior
        if (isRunning.compareAndSet(false, true) == false) {
            String msg = "Attempted to set [upgrade_mode] to [" +
                request.isEnabled() + "] from [" + MlMetadata.getMlMetadata(state).isUpgradeMode() +
                "] while previous request was processing.";
            Exception detail = new IllegalStateException(msg);
            listener.onFailure(new ElasticsearchStatusException(
                "Cannot change [upgrade_mode]. Previous request is still being processed.",
                RestStatus.TOO_MANY_REQUESTS,
                detail));
        }
        // Once we have set isRunning to true, we don't want to chance it being kept that way without the master node having to restart
        try {
            ActionListener<AcknowledgedResponse> wrappedListener = ActionListener.wrap(
                r -> {
                    isRunning.set(false);
                    listener.onResponse(r);
                },
                e -> {
                    isRunning.set(false);
                    listener.onFailure(e);
                }
            );
            final PersistentTasksCustomMetaData tasksCustomMetaData = state.metaData().custom(PersistentTasksCustomMetaData.TYPE);
            final Collection<PersistentTask<?>> allJobTasks =
                tasksCustomMetaData.findTasks(MlTasks.JOB_TASK_NAME, task -> true);

            // <4> We have unassigned the tasks, respond to the listener.
            ActionListener<List<PersistentTask<?>>> unassignPersistentTasksListener = ActionListener.wrap(
                unassigndPersistentTasks -> {
                    // TODO Is there some other besides the datafeeds fully stopping?
                    awaitCondition(
                        () -> client.admin().cluster().prepareListTasks().setActions(DATAFEED_TASK_NAME + "[c]").get().getTasks().isEmpty(),
                        "datafeed tasks to fully stop.",
                        request.timeout(),
                        wrappedListener);
                },
                wrappedListener::onFailure
            );

            // <3> After isolating the datafeeds, unassign the tasks
            ActionListener<List<IsolateDatafeedAction.Response>> isolateDatafeedListener = ActionListener.wrap(
                isolatedDatafeeds -> unassignPersistentTasks(tasksCustomMetaData, unassignPersistentTasksListener),
                wrappedListener::onFailure
            );

           /*
             <2> Handle the cluster response and act accordingly
             <.1>
                 If we are enabling the option, we need to isolate the datafeeds so we can unassign the ML Jobs
             </.1>
             <.2>
                 If we are disabling the option, we need to wait to make sure all the jobs get reallocated to an appropriate node
                 before returning to the user.

                 We don't want to return to the user unless we would be ready to handle a call against this endpoint immediately again.
                 Don't want to leave the jobs in a weird reassignment state.
             </.2>
             </2>
            */
            ActionListener<AcknowledgedResponse> clusterStateUpdateListener = ActionListener.wrap(
                acknowledgedResponse -> {
                    // State change was not acknowledged, we either timed out or ran into some exception
                    // We should not continue and alert failure to the end user
                    if (acknowledgedResponse.isAcknowledged() == false) {
                        wrappedListener.onFailure(new ElasticsearchTimeoutException("Unknown error occurred while updating cluster state"));
                        return;
                    }
                    // Did we change from disabled -> enabled?
                    if (request.isEnabled()) {
                        isolateDatafeeds(tasksCustomMetaData, isolateDatafeedListener);
                    } else {
                        // We disabled the setting, we should simply wait for all jobs to not have the `AWAITING_UPGRADE` assignment
                        List<String> jobIdsRestarted = allJobTasks.stream()
                            .filter(persistentTask -> persistentTask.getAssignment().equals(AWAITING_UPGRADE))
                            .map(persistentTask -> persistentTask.getId().substring(JOB_TASK_ID_PREFIX.length()))
                            .collect(Collectors.toList());
                        GetJobsStatsAction.Request jobStatsRequest =
                            new GetJobsStatsAction.Request(Strings.collectionToCommaDelimitedString(jobIdsRestarted));
                        awaitCondition(() -> jobsAwaitingUpgrade(jobStatsRequest),
                            "jobs to be reassigned to nodes.",
                            request.timeout(),
                            wrappedListener);
                    }
                },
                wrappedListener::onFailure
            );

            //<1> Change MlMetadata to indicate that upgrade_mode is now enabled
            clusterService.submitStateUpdateTask("ml-set-upgrade-mode",
                new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, clusterStateUpdateListener) {

                    @Override
                    protected AcknowledgedResponse newResponse(boolean acknowledged) {
                        return new AcknowledgedResponse(acknowledged);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        MlMetadata.Builder builder = new MlMetadata.Builder(currentState.metaData().custom(MlMetadata.TYPE));
                        builder.isUpgradeMode(request.isEnabled());
                        ClusterState.Builder newState = ClusterState.builder(currentState);
                        newState.metaData(MetaData.builder(currentState.getMetaData()).putCustom(MlMetadata.TYPE, builder.build()).build());
                        return newState.build();
                    }
                });
        } finally {
            isRunning.set(false);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(SetUpgradeModeAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private boolean jobsAwaitingUpgrade(GetJobsStatsAction.Request jobStatsRequest) throws InterruptedException, ExecutionException {
        try (ThreadContext.StoredContext ignore = stashWithOrigin(client.threadPool().getThreadContext(),
            ML_ORIGIN)) {
            return client.execute(GetJobsStatsAction.INSTANCE, jobStatsRequest).get()
                .getResponse()
                .results()
                .stream()
                .anyMatch(
                    jobStats -> jobStats.getAssignmentExplanation().equals(AWAITING_UPGRADE.getExplanation()));
        }
    }

    /**
     * Unassigns all Job and Datafeed tasks.
     * <p>
     * The reason for unassigning both types is that we want the Datafeed to attempt re-assignment once `upgrade_mode` is
     * disabled.
     * <p>
     * If we do not force an allocation change for the Datafeed tasks, they will never start again, since they were isolated.
     * <p>
     * Datafeed tasks keep the state as `started` and Jobs stay `opened`
     *
     * @param tasksCustomMetaData Current state of persistent tasks
     * @param listener            Alerted when tasks are unassignd
     */
    private void unassignPersistentTasks(PersistentTasksCustomMetaData tasksCustomMetaData,
                                         ActionListener<List<PersistentTask<?>>> listener) {
        List<PersistentTask<?>> datafeedAndJobTasks = tasksCustomMetaData
            .tasks()
            .stream()
            .filter(persistentTask -> (persistentTask.getTaskName().equals(MlTasks.JOB_TASK_NAME) ||
                persistentTask.getTaskName().equals(MlTasks.DATAFEED_TASK_NAME)))
            .collect(Collectors.toList());

        TypedChainTaskExecutor<PersistentTask<?>> chainTaskExecutor =
            new TypedChainTaskExecutor<>(client.threadPool().executor(executor()), r -> true, ex -> true);

        for (PersistentTask<?> task : datafeedAndJobTasks) {
            chainTaskExecutor.add(
                chainedTask -> persistentTasksClusterService.unassignPersistentTask(task.getId(),
                    task.getAllocationId(),
                    AWAITING_UPGRADE.getExplanation(),
                    chainedTask)
            );
        }
        chainTaskExecutor.execute(listener);
    }

    private void isolateDatafeeds(PersistentTasksCustomMetaData tasksCustomMetaData,
                                  ActionListener<List<IsolateDatafeedAction.Response>> listener) {
        Set<String> datafeedsToIsolate = MlTasks.startedDatafeedIds(tasksCustomMetaData);

        TypedChainTaskExecutor<IsolateDatafeedAction.Response> isolateDatafeedsExecutor =
            new TypedChainTaskExecutor<>(client.threadPool().executor(executor()), r -> true, ex -> true);

        datafeedsToIsolate.forEach(datafeedId -> {
            IsolateDatafeedAction.Request isolationRequest = new IsolateDatafeedAction.Request(datafeedId);
            isolateDatafeedsExecutor.add(isolateListener ->
                executeAsyncWithOrigin(client, ML_ORIGIN, IsolateDatafeedAction.INSTANCE, isolationRequest, isolateListener)
            );
        });

        isolateDatafeedsExecutor.execute(listener);
    }

    private void awaitCondition(CheckedSupplier<Boolean, Exception> condition,
                                String reason,
                                TimeValue timeout,
                                ActionListener<AcknowledgedResponse> listener) {
        threadPool.generic().execute(() -> {
            try {
                long msWaited = 0;
                long tick = 1;
                boolean done = condition.get();
                while (msWaited <= timeout.millis() && done == false) {
                    Thread.sleep(tick);
                    msWaited += tick;
                    tick = Math.min(tick * 2, 1000L);
                    done = condition.get();
                }

                if (done) {
                    listener.onResponse(new AcknowledgedResponse(true));
                } else {
                    listener.onFailure(new ElasticsearchTimeoutException("Timed out waiting for " + reason));
                }
            } catch (InterruptedException e) {
                listener.onFailure(e);
            } catch (Exception e) {
                listener.onFailure(new ElasticsearchTimeoutException("Encountered unexpected error while waiting for " + reason, e));
            }
        });
    }
}
