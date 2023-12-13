/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.xpack.inference.external.http.batching.BatchingComponents;
import org.elasticsearch.xpack.inference.external.http.batching.RequestBatcherFactory;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderFactory;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public abstract class SenderService<K> implements InferenceService {
    private final SetOnce<HttpRequestSenderFactory> factory;
    private final SetOnce<ServiceComponents> serviceComponents;
    private final AtomicReference<Sender<K>> sender = new AtomicReference<>();
    private final Function<BatchingComponents, RequestBatcherFactory<K>> batchFactoryCreator;

    public SenderService(
        SetOnce<HttpRequestSenderFactory> factory,
        SetOnce<ServiceComponents> serviceComponents,
        Function<BatchingComponents, RequestBatcherFactory<K>> batchFactoryCreator
    ) {
        this.factory = Objects.requireNonNull(factory);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
        this.batchFactoryCreator = Objects.requireNonNull(batchFactoryCreator);
    }

    protected Sender<K> getSender() {
        return sender.get();
    }

    protected ServiceComponents getServiceComponents() {
        return serviceComponents.get();
    }

    @Override
    public void infer(Model model, List<String> input, Map<String, Object> taskSettings, ActionListener<InferenceServiceResults> listener) {
        init();

        doInfer(model, input, taskSettings, listener);
    }

    protected abstract void doInfer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        ActionListener<InferenceServiceResults> listener
    );

    @Override
    public void start(Model model, ActionListener<Boolean> listener) {
        init();

        doStart(model, listener);
    }

    protected void doStart(Model model, ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    private void init() {
        sender.updateAndGet(
            current -> Objects.requireNonNullElseGet(current, () -> factory.get().createSender(name(), batchFactoryCreator))
        );
        sender.get().start();
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeWhileHandlingException(sender.get());
    }
}
