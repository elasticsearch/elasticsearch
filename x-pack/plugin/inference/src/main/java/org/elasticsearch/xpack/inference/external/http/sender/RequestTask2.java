/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ListenerTimeouts;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.batching.RequestCreator;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

class RequestTask2<K> implements Task<K> {

    private final AtomicBoolean finished = new AtomicBoolean();
    private final RequestCreator<K> requestCreator;
    private final List<String> input;
    private final ActionListener<InferenceServiceResults> listener;

    RequestTask2(
        RequestCreator<K> requestCreator,
        List<String> input,
        @Nullable TimeValue timeout,
        ThreadPool threadPool,
        ActionListener<InferenceServiceResults> listener
    ) {
        this.requestCreator = Objects.requireNonNull(requestCreator);
        this.input = Objects.requireNonNull(input);
        this.listener = getListener(Objects.requireNonNull(listener), timeout, Objects.requireNonNull(threadPool));
    }

    private ActionListener<InferenceServiceResults> getListener(
        ActionListener<InferenceServiceResults> origListener,
        @Nullable TimeValue timeout,
        ThreadPool threadPool
    ) {
        ActionListener<InferenceServiceResults> notificationListener = ActionListener.wrap(result -> {
            finished.set(true);
            origListener.onResponse(result);
        }, e -> {
            finished.set(true);
            origListener.onFailure(e);
        });

        if (timeout == null) {
            return notificationListener;
        }

        return ListenerTimeouts.wrapWithTimeout(
            threadPool,
            timeout,
            threadPool.executor(UTILITY_THREAD_POOL_NAME),
            notificationListener,
            // TODO if this times out technically the retrying sender could still be retrying. We should devise a way
            // to cancel the retryer task
            (ignored) -> notificationListener.onFailure(
                new ElasticsearchTimeoutException(Strings.format("Request timed out waiting to be sent after [%s]", timeout))
            )
        );
    }

    @Override
    public boolean hasFinished() {
        return finished.get();
    }

    @Override
    public boolean shouldShutdown() {
        return false;
    }

    @Override
    public List<String> input() {
        return input;
    }

    @Override
    public ActionListener<InferenceServiceResults> listener() {
        return listener;
    }

    @Override
    public void onRejection(Exception e) {
        listener.onFailure(e);
    }

    @Override
    public RequestCreator<K> requestCreator() {
        return requestCreator;
    }
}
