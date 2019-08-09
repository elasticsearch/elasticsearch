/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client;

import org.apache.http.client.methods.HttpRequestBase;

import java.util.concurrent.CancellationException;

/**
 * Represents an operation that can be cancelled.
 * Returned when executing async requests through {@link RestClient#performRequestAsync(Request, ResponseListener)}, so that the request
 * can be cancelled if needed.
 */
public class Cancellable {

    static final Cancellable NO_OP = new Cancellable(null) {
        @Override
        public synchronized void cancel() {
        }

        @Override
        synchronized void runIfNotCancelled(Runnable runnable) {
            throw new UnsupportedOperationException();
        }
    };

    static Cancellable fromRequest(HttpRequestBase httpRequest) {
        return new Cancellable(httpRequest);
    }

    private final HttpRequestBase httpRequest;

    private Cancellable(HttpRequestBase httpRequest) {
        this.httpRequest = httpRequest;
    }

    /**
     * Cancels the on-going request that is associated with the current instance of {@link Cancellable}.
     */
    public synchronized void cancel() {
        this.httpRequest.abort();
    }

    /**
     * Executes some arbitrary code iff the on-going request has not been cancelled, otherwise it throws {@link CancellationException}.
     * This is needed to guarantee that cancelling a request works correctly even in case {@link #cancel()} is called between different
     * attempts of the same request. If the request has already been cancelled we don't go ahead, otherwise we run the provided
     * {@link Runnable} which will reset the request and send the next attempt.
     */
    synchronized void runIfNotCancelled(Runnable runnable) {
        if (this.httpRequest.isAborted()) {
            throw new CancellationException("request was cancelled");
        }
        runnable.run();
    }
}
