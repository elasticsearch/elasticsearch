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
package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

class TcpTransportHandshaker {

    static final String HANDSHAKE_ACTION_NAME = "internal:tcp/handshake";
    private final ConcurrentMap<Long, HandshakeResponseHandler> pendingHandshakes = new ConcurrentHashMap<>();
    private final CounterMetric numHandshakes = new CounterMetric();

    private final Version version;
    private final ThreadPool threadPool;
    private final HandshakeSender handshakeSender;

    TcpTransportHandshaker(Version version, ThreadPool threadPool, HandshakeSender handshakeSender) {
        this.version = version;
        this.threadPool = threadPool;
        this.handshakeSender = handshakeSender;
    }

    void sendHandshake(long requestId, DiscoveryNode node, TcpChannel channel, TimeValue timeout, ActionListener<Version> listener) {
        numHandshakes.inc();
        final HandshakeResponseHandler handler = new HandshakeResponseHandler(requestId, version, listener);
        pendingHandshakes.put(requestId, handler);
        channel.addCloseListener(ActionListener.wrap(
            () -> handler.handleLocalException(new TransportException("handshake failed because connection reset"))));
        boolean success = false;
        try {
            // for the request we use the minCompatVersion since we don't know what's the version of the node we talk to
            // we also have no payload on the request but the response will contain the actual version of the node we talk
            // to as the payload.
            final Version minCompatVersion = version.minimumCompatibilityVersion();
            handshakeSender.sendHandshake(node, channel, requestId, minCompatVersion);

            threadPool.schedule(timeout, ThreadPool.Names.GENERIC,
                () -> handler.handleLocalException(new ConnectTransportException(node, "handshake_timeout[" + timeout + "]")));
            success = true;
        } catch (Exception e) {
            handler.handleLocalException(new SendRequestTransportException(node, HANDSHAKE_ACTION_NAME, e));
        } finally {
            if (success == false) {
                TransportResponseHandler<?> removed = pendingHandshakes.remove(requestId);
                assert removed == null : "Handshake should not be pending if exception was thrown";
            }
        }
    }

    TransportResponse createHandshakeResponse() {
        return new VersionHandshakeResponse(version);
    }

    TransportResponseHandler<?> removeHandlerForHandshake(long requestId) {
        return pendingHandshakes.remove(requestId);
    }

    int getNumPendingHandshakes() {
        return pendingHandshakes.size();
    }

    long getNumHandshakes() {
        return numHandshakes.count();
    }

    private class HandshakeResponseHandler implements TransportResponseHandler<VersionHandshakeResponse> {

        private final long requestId;
        private final Version currentVersion;
        private final ActionListener<Version> listener;
        private final AtomicBoolean isDone = new AtomicBoolean(false);

        private HandshakeResponseHandler(long requestId, Version currentVersion, ActionListener<Version> listener) {
            this.requestId = requestId;
            this.currentVersion = currentVersion;
            this.listener = listener;
        }

        @Override
        public VersionHandshakeResponse read(StreamInput in) throws IOException {
            return new VersionHandshakeResponse(in);
        }

        @Override
        public void handleResponse(VersionHandshakeResponse response) {
            if (isDone.compareAndSet(false, true)) {
                Version version = response.version;
                if (currentVersion.isCompatible(version) == false) {
                    listener.onFailure(new IllegalStateException("Received message from unsupported version: [" + version
                        + "] minimal compatible version is: [" + currentVersion.minimumCompatibilityVersion() + "]"));
                } else {
                    listener.onResponse(version);
                }
            }
        }

        @Override
        public void handleException(TransportException e) {
            if (isDone.compareAndSet(false, true)) {
                listener.onFailure(new IllegalStateException("handshake failed", e));
            }
        }

        public void handleLocalException(TransportException e) {
            if (pendingHandshakes.remove(requestId) != null && isDone.compareAndSet(false, true)) {
                listener.onFailure(e);
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    private static final class VersionHandshakeResponse extends TransportResponse {

        private final Version version;

        private VersionHandshakeResponse(Version version) {
            this.version = version;
        }

        private VersionHandshakeResponse(StreamInput in) throws IOException {
            super.readFrom(in);
            version = Version.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            assert version != null;
            Version.writeVersion(version, out);
        }
    }

    @FunctionalInterface
    interface HandshakeSender {

        void sendHandshake(DiscoveryNode node, TcpChannel channel, long requestId, Version version) throws IOException;
    }
}
