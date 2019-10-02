/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.inference.action.InferModelAction;
import org.elasticsearch.xpack.ml.inference.action.InferenceResults;
import org.elasticsearch.xpack.ml.inference.loadingservice.Model;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.utils.TypedChainTaskExecutor;

import java.util.List;

public class TransportInferModelAction extends HandledTransportAction<InferModelAction.Request, InferModelAction.Response> {

    private final ModelLoadingService modelLoadingService;
    private final Client client;

    @Inject
    public TransportInferModelAction(String actionName,
                                     TransportService transportService,
                                     ActionFilters actionFilters,
                                     ModelLoadingService modelLoadingService,
                                     Client client) {
        super(actionName, transportService, actionFilters, InferModelAction.Request::new);
        this.modelLoadingService = modelLoadingService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, InferModelAction.Request request, ActionListener<InferModelAction.Response> listener) {

        ActionListener<List<InferenceResults>> inferenceCompleteListener = ActionListener.wrap(
            inferenceResponse -> listener.onResponse(new InferModelAction.Response(inferenceResponse)),
            listener::onFailure
        );

        ActionListener<Model> getModelListener = ActionListener.wrap(
            model -> {
                TypedChainTaskExecutor<InferenceResults> typedChainTaskExecutor =
                    new TypedChainTaskExecutor<>(client.threadPool().executor(ThreadPool.Names.SAME),
                    // run through all tasks
                    r -> true,
                    // Always fail immediately and return an error
                    ex -> true);
                if (request.getTopClasses() != null) {
                    request.getObjectsToInfer().forEach(stringObjectMap ->
                        typedChainTaskExecutor.add(chainedTask -> model.confidence(stringObjectMap, request.getTopClasses(), chainedTask))
                    );
                } else {
                    request.getObjectsToInfer().forEach(stringObjectMap ->
                        typedChainTaskExecutor.add(chainedTask -> model.infer(stringObjectMap, chainedTask))
                    );
                }
                typedChainTaskExecutor.execute(inferenceCompleteListener);
            },
            listener::onFailure
        );

        this.modelLoadingService.getModelAndCache(request.getModelId(), request.getModelVersion(), getModelListener);
    }
}
