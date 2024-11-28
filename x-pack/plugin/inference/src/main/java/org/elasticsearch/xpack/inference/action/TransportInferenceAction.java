/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.common.ServiceToNodeGroupingClusterStateListener;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.telemetry.InferenceStats;
import org.elasticsearch.xpack.inference.telemetry.InferenceTimer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.telemetry.InferenceStats.modelAttributes;
import static org.elasticsearch.xpack.inference.telemetry.InferenceStats.responseAttributes;

public class TransportInferenceAction extends HandledTransportAction<InferenceAction.Request, InferenceAction.Response> {
    private static final Logger log = LogManager.getLogger(TransportInferenceAction.class);
    private static final String STREAMING_INFERENCE_TASK_TYPE = "streaming_inference";
    private static final String STREAMING_TASK_ACTION = "xpack/inference/streaming_inference[n]";

    private final TransportService transportService;
    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;
    private final InferenceStats inferenceStats;
    private final StreamingTaskManager streamingTaskManager;
    private final ServiceToNodeGroupingClusterStateListener serviceToNodeGroupingClusterStateListener;
    private final NodeClient nodeClient;
    private final ThreadPool threadPool;

    @Inject
    public TransportInferenceAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry,
        InferenceStats inferenceStats,
        StreamingTaskManager streamingTaskManager,
        ServiceToNodeGroupingClusterStateListener serviceToNodeGroupingClusterStateListener,
        NodeClient nodeClient,
        ThreadPool threadPool
    ) {
        super(InferenceAction.NAME, transportService, actionFilters, InferenceAction.Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
        this.inferenceStats = inferenceStats;
        this.streamingTaskManager = streamingTaskManager;
        this.serviceToNodeGroupingClusterStateListener = serviceToNodeGroupingClusterStateListener;
        this.nodeClient = nodeClient;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {
        var timer = InferenceTimer.start();
        // TODO: check, if is streaming as soon as streaming on transport level is supported
        boolean isCompletionRequest = TaskType.COMPLETION.equals(request.getTaskType());

        var getModelListener = ActionListener.wrap((UnparsedModel unparsedModel) -> {
            var service = serviceRegistry.getService(unparsedModel.service());
            if (service.isEmpty()) {
                var e = unknownServiceException(unparsedModel.service(), request.getInferenceEntityId());
                recordMetrics(unparsedModel, timer, e);
                listener.onFailure(e);
                return;
            }

            if (request.getTaskType().isAnyOrSame(unparsedModel.taskType()) == false) {
                // not the wildcard task type and not the model task type
                var e = incompatibleTaskTypeException(request.getTaskType(), unparsedModel.taskType());
                recordMetrics(unparsedModel, timer, e);
                listener.onFailure(e);
                return;
            }


            var model = service.get()
                .parsePersistedConfigWithSecrets(
                    unparsedModel.inferenceEntityId(),
                    unparsedModel.taskType(),
                    unparsedModel.settings(),
                    unparsedModel.secrets()
                );

            if(isCompletionRequest){
                String serviceName = unparsedModel.service();
                Map<String, DiscoveryNode> serviceToNodeGrouping = serviceToNodeGroupingClusterStateListener.serviceToNodeGrouping();
                boolean currentNodeShouldHandleRequest;

                if (serviceToNodeGrouping == null) {
                    // Grouping not yet calculated, current node should handle the request
                    currentNodeShouldHandleRequest = true;
                } else {
                    String localNodeId = nodeClient.getLocalNodeId();
                    DiscoveryNode nodeToHandleRequest = serviceToNodeGrouping.get(serviceName);

                    currentNodeShouldHandleRequest = nodeToHandleRequest != null && nodeToHandleRequest.getId().equals(localNodeId);
                }

                if(currentNodeShouldHandleRequest){
                    // Inference on current node
                    inferOnServiceWithMetrics(model, request, service.get(), timer, listener);
                } else {
                        // Reroute to node responsible for service type
                        log.info("Rerouting request from node [{}] to node [{}] for service [{}]", nodeClient.getLocalNodeId(), serviceToNodeGrouping.get(serviceName).getId(), serviceName);

                        transportService.sendRequest(
                            serviceToNodeGrouping.get(serviceName),
                            InferenceAction.NAME,
                            request,
                            // TODO: here we would need to stream on the transport level
                            new TransportResponseHandler<InferenceAction.Response>() {
                                @Override
                                public Executor executor() {
                                    return threadPool.executor(InferencePlugin.UTILITY_THREAD_POOL_NAME);
                                }

                                @Override
                                public void handleResponse(InferenceAction.Response response) {
                                    listener.onResponse(response);
                                }

                                @Override
                                public void handleException(TransportException exp) {
                                    listener.onFailure(exp);
                                }

                                @Override
                                public InferenceAction.Response read(StreamInput in) throws IOException {
                                    return new InferenceAction.Response(in);
                                }
                            }
                        );
                }
            } else {
                // For other task types the current node will always handle the request
                inferOnServiceWithMetrics(model, request, service.get(), timer, listener);
            }
        }, e -> {
            try {
                inferenceStats.inferenceDuration().record(timer.elapsedMillis(), responseAttributes(e));
            } catch (Exception metricsException) {
                log.atDebug().withThrowable(metricsException).log("Failed to record metrics when the model is missing, dropping metrics");
            }
            listener.onFailure(e);
        });

        modelRegistry.getModelWithSecrets(request.getInferenceEntityId(), getModelListener);
    }

    private void recordMetrics(UnparsedModel model, InferenceTimer timer, @Nullable Throwable t) {
        try {
            inferenceStats.inferenceDuration().record(timer.elapsedMillis(), responseAttributes(model, t));
        } catch (Exception e) {
            log.atDebug().withThrowable(e).log("Failed to record metrics with an unparsed model, dropping metrics");
        }
    }

    private void inferOnServiceWithMetrics(
        Model model,
        InferenceAction.Request request,
        InferenceService service,
        InferenceTimer timer,
        ActionListener<InferenceAction.Response> listener
    ) {
        inferenceStats.requestCount().incrementBy(1, modelAttributes(model));
        inferOnService(model, request, service, ActionListener.wrap(inferenceResults -> {
            if (request.isStreaming()) {
                var taskProcessor = streamingTaskManager.<ChunkedToXContent>create(STREAMING_INFERENCE_TASK_TYPE, STREAMING_TASK_ACTION);
                inferenceResults.publisher().subscribe(taskProcessor);

                var instrumentedStream = new PublisherWithMetrics(timer, model);
                taskProcessor.subscribe(instrumentedStream);

                listener.onResponse(new InferenceAction.Response(inferenceResults, instrumentedStream));
            } else {
                recordMetrics(model, timer, null);
                listener.onResponse(new InferenceAction.Response(inferenceResults));
            }
        }, e -> {
            recordMetrics(model, timer, e);
            listener.onFailure(e);
        }));
    }

    private void recordMetrics(Model model, InferenceTimer timer, @Nullable Throwable t) {
        try {
            inferenceStats.inferenceDuration().record(timer.elapsedMillis(), responseAttributes(model, t));
        } catch (Exception e) {
            log.atDebug().withThrowable(e).log("Failed to record metrics with a parsed model, dropping metrics");
        }
    }

    private void inferOnService(
        Model model,
        InferenceAction.Request request,
        InferenceService service,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (request.isStreaming() == false || service.canStream(request.getTaskType())) {
            service.infer(
                model,
                request.getQuery(),
                request.getInput(),
                request.isStreaming(),
                request.getTaskSettings(),
                request.getInputType(),
                request.getInferenceTimeout(),
                listener
            );
        } else {
            listener.onFailure(unsupportedStreamingTaskException(request, service));
        }
    }

    private ElasticsearchStatusException unsupportedStreamingTaskException(InferenceAction.Request request, InferenceService service) {
        var supportedTasks = service.supportedStreamingTasks();
        if (supportedTasks.isEmpty()) {
            return new ElasticsearchStatusException(
                format("Streaming is not allowed for service [%s].", service.name()),
                RestStatus.METHOD_NOT_ALLOWED
            );
        } else {
            var validTasks = supportedTasks.stream().map(TaskType::toString).collect(Collectors.joining(","));
            return new ElasticsearchStatusException(
                format(
                    "Streaming is not allowed for service [%s] and task [%s]. Supported tasks: [%s]",
                    service.name(),
                    request.getTaskType(),
                    validTasks
                ),
                RestStatus.METHOD_NOT_ALLOWED
            );
        }
    }

    private static ElasticsearchStatusException unknownServiceException(String service, String inferenceId) {
        return new ElasticsearchStatusException("Unknown service [{}] for model [{}]. ", RestStatus.BAD_REQUEST, service, inferenceId);
    }

    private static ElasticsearchStatusException incompatibleTaskTypeException(TaskType requested, TaskType expected) {
        return new ElasticsearchStatusException(
            "Incompatible task_type, the requested type [{}] does not match the model type [{}]",
            RestStatus.BAD_REQUEST,
            requested,
            expected
        );
    }

    private class PublisherWithMetrics extends DelegatingProcessor<ChunkedToXContent, ChunkedToXContent> {
        private final InferenceTimer timer;
        private final Model model;

        private PublisherWithMetrics(InferenceTimer timer, Model model) {
            this.timer = timer;
            this.model = model;
        }

        @Override
        protected void next(ChunkedToXContent item) {
            downstream().onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            recordMetrics(model, timer, throwable);
            super.onError(throwable);
        }

        @Override
        protected void onCancel() {
            recordMetrics(model, timer, null);
            super.onCancel();
        }

        @Override
        public void onComplete() {
            recordMetrics(model, timer, null);
            super.onComplete();
        }
    }

}
