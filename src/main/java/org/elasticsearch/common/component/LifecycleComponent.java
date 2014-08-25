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

package org.elasticsearch.common.component;

import org.elasticsearch.ElasticsearchException;

/**
 *
 */
public interface LifecycleComponent<T> extends CloseableComponent {

    Lifecycle.State lifecycleState();

    void addLifecycleListener(LifecycleListener listener);

    void removeLifecycleListener(LifecycleListener listener);

    /**
     * Disable a component. <br />
     * This can be called before {@link #stop()} to put a component into the disabled state.
     *
     * In the disabled state a component will reject new operations and wait for all pending operations to complete.
     */
    T disable() throws ElasticsearchException;

    /**
     * Starts a component. <br />
     * If the component was just disabled it will re-enable the component again but not invoke a full start
     * so handlers like {@link org.elasticsearch.common.component.LifecycleListener#beforeStart()} aren't called.
     */
    T start() throws ElasticsearchException;

    T stop() throws ElasticsearchException;
}
