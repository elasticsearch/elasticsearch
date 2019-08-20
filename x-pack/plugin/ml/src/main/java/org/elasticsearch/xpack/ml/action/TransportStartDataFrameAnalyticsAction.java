/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.EstimateMemoryUsageAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsManager;
import org.elasticsearch.xpack.ml.dataframe.MappingsMerger;
import org.elasticsearch.xpack.ml.dataframe.SourceDestValidator;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.job.JobNodeSelector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.ml.MlTasks.AWAITING_UPGRADE;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_OPEN_JOBS_PER_NODE;

/**
 * Starts the persistent task for running data frame analytics.
 *
 * TODO Add to the upgrade mode action
 */
public class TransportStartDataFrameAnalyticsAction
    extends TransportMasterNodeAction<StartDataFrameAnalyticsAction.Request, AcknowledgedResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportStartDataFrameAnalyticsAction.class);

    private final XPackLicenseState licenseState;
    private final Client client;
    private final PersistentTasksService persistentTasksService;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final MlMemoryTracker memoryTracker;

    @Inject
    public TransportStartDataFrameAnalyticsAction(TransportService transportService, Client client, ClusterService clusterService,
                                                  ThreadPool threadPool, ActionFilters actionFilters, XPackLicenseState licenseState,
                                                  IndexNameExpressionResolver indexNameExpressionResolver,
                                                  PersistentTasksService persistentTasksService,
                                                  DataFrameAnalyticsConfigProvider configProvider, MlMemoryTracker memoryTracker) {
        super(StartDataFrameAnalyticsAction.NAME, transportService, clusterService, threadPool, actionFilters,
                StartDataFrameAnalyticsAction.Request::new, indexNameExpressionResolver);
        this.licenseState = licenseState;
        this.client = client;
        this.persistentTasksService = persistentTasksService;
        this.configProvider = configProvider;
        this.memoryTracker = memoryTracker;
    }

    @Override
    protected String executor() {
        // This api doesn't do heavy or blocking operations (just delegates PersistentTasksService),
        // so we can do this on the network thread
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(StartDataFrameAnalyticsAction.Request request, ClusterState state) {
        // We only delegate here to PersistentTasksService, but if there is a metadata writeblock,
        // then delegating to PersistentTasksService doesn't make a whole lot of sense,
        // because PersistentTasksService will then fail.
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Task task, StartDataFrameAnalyticsAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        if (licenseState.isMachineLearningAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

        // Wait for analytics to be started
        ActionListener<PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams>> waitForAnalyticsToStart =
            new ActionListener<PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams>>() {
                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams> task) {
                    waitForAnalyticsStarted(task, request.getTimeout(), listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ResourceAlreadyExistsException) {
                        e = new ElasticsearchStatusException("Cannot open data frame analytics [" + request.getId() +
                            "] because it has already been opened", RestStatus.CONFLICT, e);
                    }
                    listener.onFailure(e);
                }
            };

        AtomicReference<DataFrameAnalyticsConfig> configHolder = new AtomicReference<>();

        // Start persistent task
        ActionListener<Void> memoryRequirementRefreshListener = ActionListener.wrap(
            aVoid -> {
                StartDataFrameAnalyticsAction.TaskParams taskParams = new StartDataFrameAnalyticsAction.TaskParams(
                    request.getId(), configHolder.get().getVersion());
                persistentTasksService.sendStartRequest(MlTasks.dataFrameAnalyticsTaskId(request.getId()),
                    MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, taskParams, waitForAnalyticsToStart);
            },
            listener::onFailure
        );

        // Tell the job tracker to refresh the memory requirement for this job and all other jobs that have persistent tasks
        ActionListener<EstimateMemoryUsageAction.Response> estimateMemoryUsageListener = ActionListener.wrap(
            estimateMemoryUsageResponse -> {
                // Validate that model memory limit is sufficient to run the analysis
                if (estimateMemoryUsageResponse.getExpectedMemoryUsageWithOnePartition() != null &&
                        configHolder.get().getModelMemoryLimit().compareTo(
                            estimateMemoryUsageResponse.getExpectedMemoryUsageWithOnePartition()) < 0) {
                    ElasticsearchStatusException e =
                        ExceptionsHelper.badRequestException(
                            "Cannot start because the configured model memory limit [{}] is lower than the expected memory usage [{}]",
                            configHolder.get().getModelMemoryLimit(), estimateMemoryUsageResponse.getExpectedMemoryUsageWithOnePartition());
                    listener.onFailure(e);
                    return;
                }
                // Refresh memory requirement for jobs
                memoryTracker.addDataFrameAnalyticsJobMemoryAndRefreshAllOthers(
                    request.getId(), configHolder.get().getModelMemoryLimit().getBytes(), memoryRequirementRefreshListener);
            },
            listener::onFailure
        );

        // Perform memory usage estimation for this config
        ActionListener<DataFrameAnalyticsConfig> configListener = ActionListener.wrap(
            config -> {
                configHolder.set(config);
                EstimateMemoryUsageAction.Request estimateMemoryUsageRequest = new EstimateMemoryUsageAction.Request(config);
                ClientHelper.executeAsyncWithOrigin(
                    client,
                    ClientHelper.ML_ORIGIN,
                    EstimateMemoryUsageAction.INSTANCE,
                    estimateMemoryUsageRequest,
                    estimateMemoryUsageListener);
            },
            listener::onFailure
        );

        // Get config
        getConfigAndValidate(request.getId(), configListener);
    }

    private void getConfigAndValidate(String id, ActionListener<DataFrameAnalyticsConfig> finalListener) {
        // Step 4. Validate mappings can be merged
        ActionListener<DataFrameAnalyticsConfig> toValidateMappingsListener = ActionListener.wrap(
            config -> MappingsMerger.mergeMappings(client, config.getHeaders(), config.getSource().getIndex(), ActionListener.wrap(
                    mappings -> finalListener.onResponse(config), finalListener::onFailure)),
            finalListener::onFailure
        );

        // Step 3. Validate dest index is empty
        ActionListener<DataFrameAnalyticsConfig> toValidateDestEmptyListener = ActionListener.wrap(
            config -> checkDestIndexIsEmptyIfExists(config, toValidateMappingsListener),
            finalListener::onFailure
        );

        // Step 2. Validate source and dest; check data extraction is possible
        ActionListener<DataFrameAnalyticsConfig> getConfigListener = ActionListener.wrap(
            config -> {
                new SourceDestValidator(clusterService.state(), indexNameExpressionResolver).check(config);
                DataFrameDataExtractorFactory.validateConfigAndSourceIndex(client, config, toValidateDestEmptyListener);
            },
            finalListener::onFailure
        );

        // Step 1. Get the config
        configProvider.get(id, getConfigListener);
    }

    private void checkDestIndexIsEmptyIfExists(DataFrameAnalyticsConfig config, ActionListener<DataFrameAnalyticsConfig> listener) {
        String destIndex = config.getDest().getIndex();
        SearchRequest destEmptySearch = new SearchRequest(destIndex);
        destEmptySearch.source().size(0);
        destEmptySearch.allowPartialSearchResults(false);
        ClientHelper.executeWithHeadersAsync(config.getHeaders(), ClientHelper.ML_ORIGIN, client, SearchAction.INSTANCE,
            destEmptySearch, ActionListener.wrap(
                searchResponse -> {
                    if (searchResponse.getHits().getTotalHits().value > 0) {
                        listener.onFailure(ExceptionsHelper.badRequestException("dest index [{}] must be empty", destIndex));
                    } else {
                        listener.onResponse(config);
                    }
                },
                e -> {
                    if (e instanceof IndexNotFoundException) {
                        listener.onResponse(config);
                    } else {
                        listener.onFailure(e);
                    }
                }
            )
        );
    }

    private void waitForAnalyticsStarted(PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams> task,
                                         TimeValue timeout, ActionListener<AcknowledgedResponse> listener) {
        AnalyticsPredicate predicate = new AnalyticsPredicate();
        persistentTasksService.waitForPersistentTaskCondition(task.getId(), predicate, timeout,

            new PersistentTasksService.WaitForPersistentTaskListener<PersistentTaskParams>() {

                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<PersistentTaskParams> persistentTask) {
                    if (predicate.exception != null) {
                        // We want to return to the caller without leaving an unassigned persistent task, to match
                        // what would have happened if the error had been detected in the "fast fail" validation
                        cancelAnalyticsStart(task, predicate.exception, listener);
                    } else {
                        listener.onResponse(new AcknowledgedResponse(true));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(new ElasticsearchException("Starting data frame analytics [" + task.getParams().getId()
                        + "] timed out after [" + timeout + "]"));
                }
        });
    }

    /**
     * Important: the methods of this class must NOT throw exceptions.  If they did then the callers
     * of endpoints waiting for a condition tested by this predicate would never get a response.
     */
    private class AnalyticsPredicate implements Predicate<PersistentTasksCustomMetaData.PersistentTask<?>> {

        private volatile Exception exception;

        @Override
        public boolean test(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
            if (persistentTask == null) {
                return false;
            }

            PersistentTasksCustomMetaData.Assignment assignment = persistentTask.getAssignment();

            // This means we are awaiting a new node to be spun up, ok to return back to the user to await node creation
            if (assignment != null && assignment.equals(JobNodeSelector.AWAITING_LAZY_ASSIGNMENT)) {
                return true;
            }

            if (assignment != null && assignment.equals(PersistentTasksCustomMetaData.INITIAL_ASSIGNMENT) == false &&
                assignment.isAssigned() == false) {
                // Assignment has failed despite passing our "fast fail" validation
                exception = new ElasticsearchStatusException("Could not start data frame analytics task, allocation explanation [" +
                    assignment.getExplanation() + "]", RestStatus.TOO_MANY_REQUESTS);
                return true;
            }
            DataFrameAnalyticsTaskState taskState = (DataFrameAnalyticsTaskState) persistentTask.getState();
            DataFrameAnalyticsState analyticsState = taskState == null ? DataFrameAnalyticsState.STOPPED : taskState.getState();
            switch (analyticsState) {
                case STARTED:
                case REINDEXING:
                case ANALYZING:
                    return true;
                case STOPPING:
                    exception = ExceptionsHelper.conflictStatusException("the task has been stopped while waiting to be started");
                    return true;
                case STOPPED:
                    return false;
                case FAILED:
                default:
                    exception = ExceptionsHelper.serverError("Unexpected task state [" + analyticsState + "] while waiting to be started");
                    return true;
            }
        }
    }

    private void cancelAnalyticsStart(
        PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams> persistentTask, Exception exception,
        ActionListener<AcknowledgedResponse> listener) {
        persistentTasksService.sendRemoveRequest(persistentTask.getId(),
            new ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>>() {
                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> task) {
                    // We succeeded in cancelling the persistent task, but the
                    // problem that caused us to cancel it is the overall result
                    listener.onFailure(exception);
                }

                @Override
                public void onFailure(Exception e) {
                    LOGGER.error("[" + persistentTask.getParams().getId() + "] Failed to cancel persistent task that could " +
                        "not be assigned due to [" + exception.getMessage() + "]", e);
                    listener.onFailure(exception);
                }
            }
        );
    }

    public static class DataFrameAnalyticsTask extends AllocatedPersistentTask implements StartDataFrameAnalyticsAction.TaskMatcher {

        private final Client client;
        private final ClusterService clusterService;
        private final DataFrameAnalyticsManager analyticsManager;
        private final StartDataFrameAnalyticsAction.TaskParams taskParams;
        @Nullable
        private volatile Long reindexingTaskId;
        private volatile boolean isStopping;

        public DataFrameAnalyticsTask(long id, String type, String action, TaskId parentTask, Map<String, String> headers,
                                      Client client, ClusterService clusterService, DataFrameAnalyticsManager analyticsManager,
                                      StartDataFrameAnalyticsAction.TaskParams taskParams) {
            super(id, type, action, MlTasks.DATA_FRAME_ANALYTICS_TASK_ID_PREFIX + taskParams.getId(), parentTask, headers);
            this.client = Objects.requireNonNull(client);
            this.clusterService = Objects.requireNonNull(clusterService);
            this.analyticsManager = Objects.requireNonNull(analyticsManager);
            this.taskParams = Objects.requireNonNull(taskParams);
        }

        public StartDataFrameAnalyticsAction.TaskParams getParams() {
            return taskParams;
        }

        public void setReindexingTaskId(Long reindexingTaskId) {
            this.reindexingTaskId = reindexingTaskId;
        }

        @Nullable
        public Long getReindexingTaskId() {
            return reindexingTaskId;
        }

        public boolean isStopping() {
            return isStopping;
        }

        @Override
        protected void onCancelled() {
            stop(getReasonCancelled(), TimeValue.ZERO);
        }

        public void stop(String reason, TimeValue timeout) {
            isStopping = true;
            if (reindexingTaskId != null) {
                cancelReindexingTask(reason, timeout);
            }
            analyticsManager.stop(this);
        }

        private void cancelReindexingTask(String reason, TimeValue timeout) {
            TaskId reindexTaskId = new TaskId(clusterService.localNode().getId(), reindexingTaskId);
            LOGGER.debug("[{}] Cancelling reindex task [{}]", taskParams.getId(), reindexTaskId);

            CancelTasksRequest cancelReindex = new CancelTasksRequest();
            cancelReindex.setTaskId(reindexTaskId);
            cancelReindex.setReason(reason);
            cancelReindex.setTimeout(timeout);
            CancelTasksResponse cancelReindexResponse = client.admin().cluster().cancelTasks(cancelReindex).actionGet();
            Throwable firstError = null;
            if (cancelReindexResponse.getNodeFailures().isEmpty() == false) {
                firstError = cancelReindexResponse.getNodeFailures().get(0).getRootCause();
            }
            if (cancelReindexResponse.getTaskFailures().isEmpty() == false) {
                firstError = cancelReindexResponse.getTaskFailures().get(0).getCause();
            }
            // There is a chance that the task is finished by the time we cancel it in which case we'll get
            // a ResourceNotFoundException which we can ignore.
            if (firstError != null && firstError instanceof ResourceNotFoundException == false) {
                throw ExceptionsHelper.serverError("[" + taskParams.getId() + "] Error cancelling reindex task", firstError);
            } else {
                LOGGER.debug("[{}] Reindex task was successfully cancelled", taskParams.getId());
            }
        }

        public void updateState(DataFrameAnalyticsState state, @Nullable String reason) {
            DataFrameAnalyticsTaskState newTaskState = new DataFrameAnalyticsTaskState(state, getAllocationId(), reason);
            updatePersistentTaskState(newTaskState, ActionListener.wrap(
                updatedTask -> LOGGER.info("[{}] Successfully update task state to [{}]", getParams().getId(), state),
                e -> LOGGER.error(new ParameterizedMessage("[{}] Could not update task state to [{}]",
                    getParams().getId(), state), e)
            ));
        }
    }

    static List<String> verifyIndicesPrimaryShardsAreActive(ClusterState clusterState, String... indexNames) {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver();
        String[] concreteIndices = resolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpen(), indexNames);
        List<String> unavailableIndices = new ArrayList<>(concreteIndices.length);
        for (String index : concreteIndices) {
            IndexRoutingTable routingTable = clusterState.getRoutingTable().index(index);
            if (routingTable == null || routingTable.allPrimaryShardsActive() == false) {
                unavailableIndices.add(index);
            }
        }
        return unavailableIndices;
    }

    public static class TaskExecutor extends PersistentTasksExecutor<StartDataFrameAnalyticsAction.TaskParams> {

        private final Client client;
        private final ClusterService clusterService;
        private final DataFrameAnalyticsManager manager;
        private final MlMemoryTracker memoryTracker;

        private volatile int maxMachineMemoryPercent;
        private volatile int maxLazyMLNodes;
        private volatile int maxOpenJobs;

        public TaskExecutor(Settings settings, Client client, ClusterService clusterService, DataFrameAnalyticsManager manager,
                            MlMemoryTracker memoryTracker) {
            super(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, MachineLearning.UTILITY_THREAD_POOL_NAME);
            this.client = Objects.requireNonNull(client);
            this.clusterService = Objects.requireNonNull(clusterService);
            this.manager = Objects.requireNonNull(manager);
            this.memoryTracker = Objects.requireNonNull(memoryTracker);
            this.maxMachineMemoryPercent = MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings);
            this.maxLazyMLNodes = MachineLearning.MAX_LAZY_ML_NODES.get(settings);
            this.maxOpenJobs = MAX_OPEN_JOBS_PER_NODE.get(settings);
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MachineLearning.MAX_MACHINE_MEMORY_PERCENT, this::setMaxMachineMemoryPercent);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.MAX_LAZY_ML_NODES, this::setMaxLazyMLNodes);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_OPEN_JOBS_PER_NODE, this::setMaxOpenJobs);
        }

        @Override
        protected AllocatedPersistentTask createTask(
            long id, String type, String action, TaskId parentTaskId,
            PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams> persistentTask,
            Map<String, String> headers) {
            return new DataFrameAnalyticsTask(id, type, action, parentTaskId, headers, client, clusterService, manager,
                persistentTask.getParams());
        }

        @Override
        public PersistentTasksCustomMetaData.Assignment getAssignment(StartDataFrameAnalyticsAction.TaskParams params,
                                                                      ClusterState clusterState) {

            // If we are waiting for an upgrade to complete, we should not assign to a node
            if (MlMetadata.getMlMetadata(clusterState).isUpgradeMode()) {
                return AWAITING_UPGRADE;
            }

            String id = params.getId();

            List<String> unavailableIndices = verifyIndicesPrimaryShardsAreActive(clusterState, AnomalyDetectorsIndex.configIndexName());
            if (unavailableIndices.size() != 0) {
                String reason = "Not opening data frame analytics job [" + id +
                    "], because not all primary shards are active for the following indices [" + String.join(",", unavailableIndices) + "]";
                LOGGER.debug(reason);
                return new PersistentTasksCustomMetaData.Assignment(null, reason);
            }

            boolean isMemoryTrackerRecentlyRefreshed = memoryTracker.isRecentlyRefreshed();
            if (isMemoryTrackerRecentlyRefreshed == false) {
                boolean scheduledRefresh = memoryTracker.asyncRefresh();
                if (scheduledRefresh) {
                    String reason = "Not opening data frame analytics job [" + id +
                        "] because job memory requirements are stale - refresh requested";
                    LOGGER.debug(reason);
                    return new PersistentTasksCustomMetaData.Assignment(null, reason);
                }
            }

            JobNodeSelector jobNodeSelector = new JobNodeSelector(clusterState, id, MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, memoryTracker,
                maxLazyMLNodes, node -> nodeFilter(node, id));
            // Pass an effectively infinite value for max concurrent opening jobs, because data frame analytics jobs do
            // not have an "opening" state so would never be rejected for causing too many jobs in the "opening" state
            return jobNodeSelector.selectNode(
                maxOpenJobs, Integer.MAX_VALUE, maxMachineMemoryPercent, isMemoryTrackerRecentlyRefreshed);
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, StartDataFrameAnalyticsAction.TaskParams params,
                                     PersistentTaskState state) {
            LOGGER.info("[{}] Starting data frame analytics", params.getId());
            DataFrameAnalyticsTaskState analyticsTaskState = (DataFrameAnalyticsTaskState) state;

            // If we are "stopping" there is nothing to do
            // If we are "failed" then we should leave the task as is; for recovery it must be force stopped.
            if (analyticsTaskState != null && analyticsTaskState.getState().isAnyOf(
                    DataFrameAnalyticsState.STOPPING, DataFrameAnalyticsState.FAILED)) {
                return;
            }

            if (analyticsTaskState == null) {
                DataFrameAnalyticsTaskState startedState = new DataFrameAnalyticsTaskState(DataFrameAnalyticsState.STARTED,
                    task.getAllocationId(), null);
                task.updatePersistentTaskState(startedState, ActionListener.wrap(
                    response -> manager.execute((DataFrameAnalyticsTask) task, DataFrameAnalyticsState.STARTED),
                    task::markAsFailed));
            } else {
                manager.execute((DataFrameAnalyticsTask)task, analyticsTaskState.getState());
            }
        }

        public static String nodeFilter(DiscoveryNode node, String id) {

            if (node.getVersion().before(StartDataFrameAnalyticsAction.TaskParams.VERSION_INTRODUCED)) {
                return "Not opening job [" + id + "] on node [" + JobNodeSelector.nodeNameAndVersion(node)
                    + "], because the data frame analytics requires a node of version ["
                    + StartDataFrameAnalyticsAction.TaskParams.VERSION_INTRODUCED + "] or higher";
            }

            return null;
        }

        void setMaxMachineMemoryPercent(int maxMachineMemoryPercent) {
            this.maxMachineMemoryPercent = maxMachineMemoryPercent;
        }

        void setMaxLazyMLNodes(int maxLazyMLNodes) {
            this.maxLazyMLNodes = maxLazyMLNodes;
        }

        void setMaxOpenJobs(int maxOpenJobs) {
            this.maxOpenJobs = maxOpenJobs;
        }
    }
}
