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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.DeployTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.DeployTrainedModelAction.TaskParams;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.inference.deployment.DeployTrainedModelState;
import org.elasticsearch.xpack.core.ml.inference.deployment.DeployTrainedModelTaskState;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.deployment.DeployTrainedModelTask;
import org.elasticsearch.xpack.ml.inference.deployment.DeploymentManager;
import org.elasticsearch.xpack.ml.job.JobNodeSelector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.task.AbstractJobPersistentTasksExecutor;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

public class TransportDeployTrainedModelAction
    extends TransportMasterNodeAction<DeployTrainedModelAction.Request, NodeAcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDeployTrainedModelAction.class);

    private final XPackLicenseState licenseState;
    private final Client client;
    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportDeployTrainedModelAction(TransportService transportService, Client client, ClusterService clusterService,
                                                ThreadPool threadPool, ActionFilters actionFilters, XPackLicenseState licenseState,
                                                IndexNameExpressionResolver indexNameExpressionResolver,
                                                PersistentTasksService persistentTasksService) {
        super(DeployTrainedModelAction.NAME, transportService, clusterService, threadPool, actionFilters,
            DeployTrainedModelAction.Request::new, indexNameExpressionResolver, NodeAcknowledgedResponse::new, ThreadPool.Names.SAME);
        this.licenseState = Objects.requireNonNull(licenseState);
        this.client = Objects.requireNonNull(client);
        this.persistentTasksService = Objects.requireNonNull(persistentTasksService);
    }

    @Override
    protected void masterOperation(Task task, DeployTrainedModelAction.Request request, ClusterState state,
                                   ActionListener<NodeAcknowledgedResponse> listener) throws Exception {
        logger.debug(() -> new ParameterizedMessage("[{}] received deploy request", request.getModelId()));
        if (licenseState.checkFeature(XPackLicenseState.Feature.MACHINE_LEARNING) == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

        ActionListener<PersistentTasksCustomMetadata.PersistentTask<TaskParams>> waitForDeploymentToStart =
            ActionListener.wrap(
                startedTask -> waitForDeploymentStarted(startedTask, request.getTimeout(), listener),
                e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        e = new ElasticsearchStatusException(
                            "Cannot start deployment [{}] because it has already been started",
                            RestStatus.CONFLICT,
                            e,
                            request.getModelId()
                        );
                    }
                    listener.onFailure(e);
                }
            );

        ActionListener<GetTrainedModelsAction.Response> getModelListener = ActionListener.wrap(
            getModelResponse -> {
                if (getModelResponse.getResources().results().size() > 1) {
                    listener.onFailure(ExceptionsHelper.badRequestException(
                        "cannot deploy more than one models at the same time; [{}] matches [{}] models]",
                        request.getModelId(), getModelResponse.getResources().results().size()));
                    return;
                }
                persistentTasksService.sendStartRequest(
                    MlTasks.deployTrainedModelTaskId(request.getModelId()),
                    MlTasks.DEPLOY_TRAINED_MODEL_TASK_NAME,
                    new TaskParams(request.getModelId()),
                    waitForDeploymentToStart
                );
            },
            listener::onFailure
        );

        GetTrainedModelsAction.Request getModelRequest = new GetTrainedModelsAction.Request(
            request.getModelId(), null, Collections.emptySet());
        client.execute(GetTrainedModelsAction.INSTANCE, getModelRequest, getModelListener);
    }

    private void waitForDeploymentStarted(PersistentTasksCustomMetadata.PersistentTask<TaskParams> task,
                                          TimeValue timeout, ActionListener<NodeAcknowledgedResponse> listener) {
        DeploymentStartedPredicate predicate = new DeploymentStartedPredicate();
        persistentTasksService.waitForPersistentTaskCondition(task.getId(), predicate, timeout,
            new PersistentTasksService.WaitForPersistentTaskListener<PersistentTaskParams>() {
                @Override
                public void onResponse(PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> persistentTask) {
                    if (predicate.exception != null) {

                    } else {
                        listener.onResponse(new NodeAcknowledgedResponse(true, predicate.node));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(DeployTrainedModelAction.Request request, ClusterState state) {
        // We only delegate here to PersistentTasksService, but if there is a metadata writeblock,
        // then delegating to PersistentTasksService doesn't make a whole lot of sense,
        // because PersistentTasksService will then fail.
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private static class DeploymentStartedPredicate implements Predicate<PersistentTasksCustomMetadata.PersistentTask<?>> {

        private volatile Exception exception;
        private volatile String node = "";
        private volatile String assignmentExplanation;

        @Override
        public boolean test(PersistentTasksCustomMetadata.PersistentTask<?> persistentTask) {
            if (persistentTask == null) {
                return false;
            }

            PersistentTasksCustomMetadata.Assignment assignment = persistentTask.getAssignment();

            String reason = "__unknown__";

            if (assignment != null) {
                if (assignment.equals(JobNodeSelector.AWAITING_LAZY_ASSIGNMENT)) {
                    return true;
                }
                if (assignment.equals(PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT) == false && assignment.isAssigned() == false) {
                    exception = new ElasticsearchStatusException("Could not start trained model deployment, allocation explanation [{}]",
                        RestStatus.TOO_MANY_REQUESTS, assignment.getExplanation());
                    return true;
                }
            }

            DeployTrainedModelTaskState taskState = (DeployTrainedModelTaskState) persistentTask.getState();
            reason = taskState != null ? taskState.getReason() : reason;
            DeployTrainedModelState deploymentState = taskState == null ? DeployTrainedModelState.DEPLOYED : taskState.getState();
            switch (deploymentState) {
                case DEPLOYED:
                    node = persistentTask.getExecutorNode();
                    return true;
                case DEPLOYING:
                case UNDEPLOYING:
                case UNDEPLOYED:
                    return false;
                default:
                    exception = ExceptionsHelper.serverError("Unexpected task state [{}] with reason [{}] while waiting to be started",
                        taskState.getState(), reason);
                    return true;
            }
        }
    }

    public static class TaskExecutor extends AbstractJobPersistentTasksExecutor<TaskParams> {

        private final DeploymentManager manager;

        public TaskExecutor(Settings settings, ClusterService clusterService, IndexNameExpressionResolver expressionResolver,
                               MlMemoryTracker memoryTracker, DeploymentManager manager) {
            super(MlTasks.DEPLOY_TRAINED_MODEL_TASK_NAME,
                MachineLearning.UTILITY_THREAD_POOL_NAME,
                settings,
                clusterService,
                memoryTracker,
                expressionResolver);
            this.manager = Objects.requireNonNull(manager);
        }

        @Override
        protected AllocatedPersistentTask createTask(
            long id, String type, String action, TaskId parentTaskId,
            PersistentTasksCustomMetadata.PersistentTask<TaskParams> persistentTask,
            Map<String, String> headers) {
            return new DeployTrainedModelTask(id, type, action, parentTaskId, headers, persistentTask.getParams());
        }

        @Override
        public PersistentTasksCustomMetadata.Assignment getAssignment(TaskParams params, ClusterState clusterState) {
            JobNodeSelector jobNodeSelector =
                new JobNodeSelector(
                    clusterState,
                    params.getModelId(),
                    MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
                    memoryTracker,
                    0,
                    node -> nodeFilter(node, params));
            PersistentTasksCustomMetadata.Assignment assignment = jobNodeSelector.selectNode(
                maxOpenJobs,
                Integer.MAX_VALUE,
                maxMachineMemoryPercent,
                maxNodeMemory,
                false,
                useAutoMemoryPercentage
            );
            return assignment;
        }

        public static String nodeFilter(DiscoveryNode node, TaskParams params) {
            String id = params.getModelId();

            if (node.getVersion().before(TaskParams.VERSION_INTRODUCED)) {
                return "Not opening job [" + id + "] on node [" + JobNodeSelector.nodeNameAndVersion(node)
                    + "], because the data frame analytics requires a node of version ["
                    + TaskParams.VERSION_INTRODUCED + "] or higher";
            }

            return null;
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, TaskParams params, PersistentTaskState state) {
            DeployTrainedModelTask deployTrainedModelTask = (DeployTrainedModelTask) task;
            deployTrainedModelTask.setDeploymentManager(manager);

            DeployTrainedModelTaskState deployingState = new DeployTrainedModelTaskState(
                DeployTrainedModelState.DEPLOYING, task.getAllocationId(), null);
            task.updatePersistentTaskState(deployingState, ActionListener.wrap(
                response -> manager.deployModel(deployTrainedModelTask),
                task::markAsFailed
            ));
        }

        @Override
        protected String[] indicesOfInterest(TaskParams params) {
            return new String[] {
                InferenceIndexConstants.INDEX_PATTERN
            };
        }

        @Override
        protected String getJobId(TaskParams params) {
            return params.getModelId();
        }
    }
}
