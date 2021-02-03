/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.usage.UsageService;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * A common base class for Rest*ActionTests. Provides access to a {@link RestController}
 * that can be used to register individual REST actions, and test request handling.
 */
public abstract class RestActionTestCase extends ESTestCase {
    private RestController controller;
    protected VerifyingClient verifyingClient;

    @Before
    public void setUpController() {
        verifyingClient = new VerifyingClient(this.getTestName());
        controller = new RestController(Collections.emptySet(), null,
            verifyingClient,
            new NoneCircuitBreakerService(),
            new UsageService());
    }

    @After
    public void tearDownController() {
        verifyingClient.close();
    }

    /**
     * A test {@link RestController}. This controller can be used to register and delegate
     * to handlers, but uses a mock client and cannot carry out the full request.
     */
    protected RestController controller() {
        return controller;
    }

    /**
     * Sends the given request to the test controller in {@link #controller()}.
     */
    protected void dispatchRequest(RestRequest request) {
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        controller.dispatchRequest(request, channel, threadContext);
    }

    /**
     * A mocked {@link org.elasticsearch.client.node.NodeClient} which can be easily reconfigured to verify arbitrary verification
     * functions, and can be reset to allow reconfiguration partway through a test without having to construct a new object.
     *
     * By default, will throw {@link AssertionError} when any execution method is called, unless configured otherwise using
     * {@link #setExecuteVerifier} or {@link #setExecuteLocallyVerifier}.
     */
    public static class VerifyingClient extends NoOpNodeClient {
        AtomicReference<BiConsumer<ActionType<?>, ActionRequest>> executeVerifier = new AtomicReference<>();
        AtomicReference<BiFunction<ActionType<?>, ActionRequest, ActionResponse>> executeLocallyVerifier = new AtomicReference<>();

        public VerifyingClient(String testName) {
            super(testName);
            reset();
        }

        @Override
        public String getLocalNodeId() {
            return "test_node_id";
        }

        /**
         * Clears any previously set verifier functions set by {@link #setExecuteVerifier} and/or
         * {@link #setExecuteLocallyVerifier}. These functions are replaced with functions which will throw an
         * {@link AssertionError} if called.
         */
        public void reset() {
            executeVerifier.set((arg1, arg2) -> {
                throw new AssertionError();
            });
            executeLocallyVerifier.set((arg1, arg2) -> {
                throw new AssertionError();
            });
        }

        /**
         * Sets the function that will be called when {@link #doExecute(ActionType, ActionRequest, ActionListener)} is called. The given
         * function should return either a subclass of {@link ActionResponse} or {@code null}.
         * @param verifier A function which is called in place of {@link #doExecute(ActionType, ActionRequest, ActionListener)}
         */
        public void setExecuteVerifier(BiConsumer<ActionType<?>, ActionRequest> verifier) {
            executeVerifier.set(verifier);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse>
        void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
            executeVerifier.get().accept(action, request);
            listener.onResponse(null);
        }

        /**
         * Sets the function that will be called when {@link #executeLocally(ActionType, ActionRequest, TaskListener)}is called. The given
         * function should return either a subclass of {@link ActionResponse} or {@code null}.
         * @param verifier A function which is called in place of {@link #executeLocally(ActionType, ActionRequest, TaskListener)}
         */
        public void setExecuteLocallyVerifier(BiFunction<ActionType<?>, ActionRequest, ActionResponse> verifier) {
            executeLocallyVerifier.set(verifier);
        }

        private static final AtomicLong taskIdGenerator = new AtomicLong(0L);

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse>
        Task executeLocally(ActionType<Response> action, Request request, ActionListener<Response> listener) {
            @SuppressWarnings("unchecked") // Callers are responsible for lining this up
            Response response = (Response) executeLocallyVerifier.get().apply(action, request);
            listener.onResponse(response);
            return new Task(taskIdGenerator.incrementAndGet(), "transport", action.name(), "", request.getParentTask(),
                    Collections.emptyMap());
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse>
        Task executeLocally(ActionType<Response> action, Request request, TaskListener<Response> listener) {
            @SuppressWarnings("unchecked") // Callers are responsible for lining this up
            Response response = (Response) executeLocallyVerifier.get().apply(action, request);
            listener.onResponse(null, response);
            return null;
        }

    }
}
