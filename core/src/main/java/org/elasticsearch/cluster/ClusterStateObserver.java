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

package org.elasticsearch.cluster;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.ClusterStateStatus;
import org.elasticsearch.cluster.service.ClusterServiceState;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A utility class which simplifies interacting with the cluster state in cases where
 * one tries to take action based on the current state but may want to wait for a new state
 * and retry upon failure.
 */
public class ClusterStateObserver {

    protected final Logger logger;

    public final ChangePredicate MATCH_ALL_CHANGES_PREDICATE = new EventPredicate() {

        @Override
        public boolean apply(ClusterServiceState previousState, ClusterServiceState currentState) {
            return previousState.getClusterState().version() != currentState.getClusterState().version();
        }
    };

    private final ClusterService clusterService;
    private final ThreadContext contextHolder;
    volatile TimeValue timeOutValue;


    final AtomicReference<ClusterServiceState> lastObservedState;
    final TimeoutClusterStateListener clusterStateListener = new ObserverClusterStateListener();
    // observingContext is not null when waiting on cluster state changes
    final AtomicReference<ObservingContext> observingContext = new AtomicReference<>(null);
    volatile Long startTimeNS;
    volatile boolean timedOut;


    public ClusterStateObserver(ClusterService clusterService, Logger logger, ThreadContext contextHolder) {
        this(clusterService, new TimeValue(60000), logger, contextHolder);
    }

    /**
     * @param timeout        a global timeout for this observer. After it has expired the observer
     *                       will fail any existing or new #waitForNextChange calls. Set to null
     *                       to wait indefinitely
     */
    public ClusterStateObserver(ClusterService clusterService, @Nullable TimeValue timeout, Logger logger, ThreadContext contextHolder) {
        this.clusterService = clusterService;
        this.lastObservedState = new AtomicReference<>(clusterService.clusterServiceState());
        this.timeOutValue = timeout;
        if (timeOutValue != null) {
            this.startTimeNS = System.nanoTime();
        }
        this.logger = logger;
        this.contextHolder = contextHolder;
    }

    /** last cluster state and status observed by this observer. Note that this may not be the current one */
    public ClusterServiceState observedState() {
        ClusterServiceState state = lastObservedState.get();
        assert state != null;
        return state;
    }

    /** indicates whether this observer has timedout */
    public boolean isTimedOut() {
        return timedOut;
    }

    public void waitForNextChange(Listener listener) {
        waitForNextChange(listener, MATCH_ALL_CHANGES_PREDICATE);
    }

    public void waitForNextChange(Listener listener, @Nullable TimeValue timeOutValue) {
        waitForNextChange(listener, MATCH_ALL_CHANGES_PREDICATE, timeOutValue);
    }

    public void waitForNextChange(Listener listener, ChangePredicate changePredicate) {
        waitForNextChange(listener, changePredicate, null);
    }

    /**
     * Wait for the next cluster state which satisfies changePredicate
     *
     * @param listener        callback listener
     * @param changePredicate predicate to check whether cluster state changes are relevant and the callback should be called
     * @param timeOutValue    a timeout for waiting. If null the global observer timeout will be used.
     */
    public void waitForNextChange(Listener listener, ChangePredicate changePredicate, @Nullable TimeValue timeOutValue) {

        if (observingContext.get() != null) {
            throw new ElasticsearchException("already waiting for a cluster state change");
        }

        Long timeoutTimeLeftMS;
        if (timeOutValue == null) {
            timeOutValue = this.timeOutValue;
            if (timeOutValue != null) {
                long timeSinceStartMS = TimeValue.nsecToMSec(System.nanoTime() - startTimeNS);
                timeoutTimeLeftMS = timeOutValue.millis() - timeSinceStartMS;
                if (timeoutTimeLeftMS <= 0L) {
                    // things have timeout while we were busy -> notify
                    logger.trace("observer timed out. notifying listener. timeout setting [{}], time since start [{}]", timeOutValue, new TimeValue(timeSinceStartMS));
                    // update to latest, in case people want to retry
                    timedOut = true;
                    lastObservedState.set(clusterService.clusterServiceState());
                    listener.onTimeout(timeOutValue);
                    return;
                }
            } else {
                timeoutTimeLeftMS = null;
            }
        } else {
            this.startTimeNS = System.nanoTime();
            this.timeOutValue = timeOutValue;
            timeoutTimeLeftMS = timeOutValue.millis();
            timedOut = false;
        }

        // sample a new state
        ClusterServiceState newState = clusterService.clusterServiceState();
        ClusterServiceState lastState = lastObservedState.get();
        if (changePredicate.apply(lastState, newState)) {
            // good enough, let's go.
            logger.trace("observer: sampled state accepted by predicate ({})", newState);
            lastObservedState.set(newState);
            listener.onNewClusterState(newState.getClusterState());
        } else {
            logger.trace("observer: sampled state rejected by predicate ({}). adding listener to ClusterService", newState);
            ObservingContext context = new ObservingContext(new ContextPreservingListener(listener, contextHolder.newStoredContext()), changePredicate);
            if (!observingContext.compareAndSet(null, context)) {
                throw new ElasticsearchException("already waiting for a cluster state change");
            }
            clusterService.add(timeoutTimeLeftMS == null ? null : new TimeValue(timeoutTimeLeftMS), clusterStateListener);
        }
    }

    /**
     * reset this observer to the give cluster state. Any pending waits will be canceled.
     */
    public void reset(ClusterServiceState state) {
        if (observingContext.getAndSet(null) != null) {
            clusterService.remove(clusterStateListener);
        }
        lastObservedState.set(state);
    }

    class ObserverClusterStateListener implements TimeoutClusterStateListener {

        @Override
        public void clusterServiceStateChanged(ClusterServiceState previousState, ClusterServiceState currentState) {
            ObservingContext context = observingContext.get();
            if (context == null) {
                // No need to remove listener as it is the responsibility of the thread that set observingContext to null
                return;
            }
            if (context.changePredicate.apply(previousState, currentState)) {
                if (observingContext.compareAndSet(context, null)) {
                    clusterService.remove(this);
                    ClusterServiceState state = new ClusterServiceState(currentState.getClusterState(), ClusterStateStatus.APPLIED, null);
                    logger.trace("observer: accepting cluster state change ({})", state);
                    lastObservedState.set(state);
                    context.listener.onNewClusterState(state.getClusterState());
                } else {
                    logger.trace("observer: predicate approved change but observing context has changed - " +
                                     "ignoring (new cluster state version [{}])", currentState.getClusterState().version());
                }
            } else {
                logger.trace("observer: predicate rejected change (new cluster state version [{}])",
                    currentState.getClusterState().version());
            }
        }

        @Override
        public void postAdded() {
            ObservingContext context = observingContext.get();
            if (context == null) {
                // No need to remove listener as it is the responsibility of the thread that set observingContext to null
                return;
            }
            ClusterServiceState newState = clusterService.clusterServiceState();
            ClusterServiceState lastState = lastObservedState.get();
            if (context.changePredicate.apply(lastState, newState)) {
                // double check we're still listening
                if (observingContext.compareAndSet(context, null)) {
                    logger.trace("observer: post adding listener: accepting current cluster state ({})", newState);
                    clusterService.remove(this);
                    lastObservedState.set(newState);
                    context.listener.onNewClusterState(newState.getClusterState());
                } else {
                    logger.trace("observer: postAdded - predicate approved state but observing context has changed - ignoring ({})", newState);
                }
            } else {
                logger.trace("observer: postAdded - predicate rejected state ({})", newState);
            }
        }

        @Override
        public void onClose() {
            ObservingContext context = observingContext.getAndSet(null);

            if (context != null) {
                logger.trace("observer: cluster service closed. notifying listener.");
                clusterService.remove(this);
                context.listener.onClusterServiceClose();
            }
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            ObservingContext context = observingContext.getAndSet(null);
            if (context != null) {
                clusterService.remove(this);
                long timeSinceStartMS = TimeValue.nsecToMSec(System.nanoTime() - startTimeNS);
                logger.trace("observer: timeout notification from cluster service. timeout setting [{}], time since start [{}]", timeOutValue, new TimeValue(timeSinceStartMS));
                // update to latest, in case people want to retry
                lastObservedState.set(clusterService.clusterServiceState());
                timedOut = true;
                context.listener.onTimeout(timeOutValue);
            }
        }
    }

    public interface Listener {

        /** called when a new state is observed */
        void onNewClusterState(ClusterState state);

        /** called when the cluster service is closed */
        void onClusterServiceClose();

        void onTimeout(TimeValue timeout);
    }

    public interface ChangePredicate {

        /**
         * Called to see whether a cluster change should be accepted.
         *
         * @return true if newState should be accepted
         */
        boolean apply(ClusterServiceState previousState,
                      ClusterServiceState newState);
    }


    public abstract static class ValidationPredicate implements ChangePredicate {

        @Override
        public boolean apply(ClusterServiceState previousState, ClusterServiceState newState) {
            return (previousState.getLocalClusterState() != newState.getLocalClusterState() ||
                        previousState.getClusterStateStatus() != newState.getClusterStateStatus()) &&
                validate(newState);
        }

        protected abstract boolean validate(ClusterServiceState newState);
    }

    public abstract static class EventPredicate implements ChangePredicate {
        @Override
        public boolean apply(ClusterServiceState previousState, ClusterServiceState newState) {
            return previousState.getClusterState() != newState.getClusterState() || previousState.getClusterStateStatus() != newState.getClusterStateStatus();
        }

    }

    static class ObservingContext {
        public final Listener listener;
        public final ChangePredicate changePredicate;

        public ObservingContext(Listener listener, ChangePredicate changePredicate) {
            this.listener = listener;
            this.changePredicate = changePredicate;
        }
    }

    private static final class ContextPreservingListener implements Listener {
        private final Listener delegate;
        private final ThreadContext.StoredContext tempContext;


        private ContextPreservingListener(Listener delegate, ThreadContext.StoredContext storedContext) {
            this.tempContext = storedContext;
            this.delegate = delegate;
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            tempContext.restore();
            delegate.onNewClusterState(state);
        }

        @Override
        public void onClusterServiceClose() {
            tempContext.restore();
            delegate.onClusterServiceClose();
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            tempContext.restore();
            delegate.onTimeout(timeout);
        }
    }
}
