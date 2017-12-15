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
package org.elasticsearch.transport.nio;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

public class NioTransportPlugin extends Plugin implements NetworkPlugin {

    public static final String NIO_TRANSPORT_NAME = "nio-transport";

    @Override
    public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                          PageCacheRecycler pageCacheRecycler,
                                                          CircuitBreakerService circuitBreakerService,
                                                          NamedWriteableRegistry namedWriteableRegistry,
                                                          NetworkService networkService) {
        Settings settings1;
        if (NioTransport.NIO_WORKER_COUNT.exists(settings) == false) {
            // As this is only used for tests right now, limit the number of worker threads.
            settings1 = Settings.builder().put(settings).put(NioTransport.NIO_WORKER_COUNT.getKey(), 2).build();
        } else {
            settings1 = settings;
        }
        return Collections.singletonMap(NIO_TRANSPORT_NAME,
            () -> new NioTransport(settings1, threadPool, networkService, bigArrays, pageCacheRecycler, namedWriteableRegistry,
                circuitBreakerService));
    }
}
