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

package org.elasticsearch.discovery;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.transport.TransportService;

/**
 * A module for loading classes for node discovery.
 */
public class DiscoveryModule extends AbstractModule {

    public static final Setting<String> DISCOVERY_TYPE_SETTING =
        new Setting<>("discovery.type", "zen", Function.identity(), Property.NodeScope);
    public static final Setting<String> DISCOVERY_HOSTS_PROVIDER_SETTING =
        new Setting<>("discovery.zen.hosts_provider", DISCOVERY_TYPE_SETTING, Function.identity(), Property.NodeScope);

    private final Settings settings;
    private final UnicastHostsProvider hostsProvider;
    private final Map<String, Class<? extends Discovery>> discoveryTypes = new HashMap<>();

    public DiscoveryModule(Settings settings, TransportService transportService, NetworkService networkService,
                           List<DiscoveryPlugin> plugins) {
        this.settings = settings;
        addDiscoveryType("none", NoneDiscovery.class);
        addDiscoveryType("zen", ZenDiscovery.class);

        String discoveryType = DISCOVERY_TYPE_SETTING.get(settings);
        if (discoveryType.equals("none") == false) {
            Map<String, Supplier<UnicastHostsProvider>> hostProviders = new HashMap<>();
            hostProviders.put("zen", () -> Collections::emptyList);
            for (DiscoveryPlugin plugin : plugins) {
                plugin.getZenHostsProviders(transportService, networkService).entrySet().forEach(entry -> {
                    if (hostProviders.put(entry.getKey(), entry.getValue()) != null) {
                        throw new IllegalArgumentException("Cannot specify zen hosts provider [" + entry.getKey() + "] twice");
                    }
                });
            }
            String hostsProviderName = DISCOVERY_HOSTS_PROVIDER_SETTING.get(settings);
            Supplier<UnicastHostsProvider> hostsProviderSupplier = hostProviders.get(hostsProviderName);
            if (hostsProviderSupplier == null) {
                throw new IllegalArgumentException("Unknown zen hosts provider [" + hostsProviderName + "]");
            }
            hostsProvider = Objects.requireNonNull(hostsProviderSupplier.get());
        } else {
            hostsProvider = null;
        }
    }

    public UnicastHostsProvider getHostsProvider() {
        return hostsProvider;
    }

    /**
     * Adds a custom Discovery type.
     */
    public void addDiscoveryType(String type, Class<? extends Discovery> clazz) {
        if (discoveryTypes.containsKey(type)) {
            throw new IllegalArgumentException("discovery type [" + type + "] is already registered");
        }
        discoveryTypes.put(type, clazz);
    }

    @Override
    protected void configure() {
        String discoveryType = DISCOVERY_TYPE_SETTING.get(settings);
        Class<? extends Discovery> discoveryClass = discoveryTypes.get(discoveryType);
        if (discoveryClass == null) {
            throw new IllegalArgumentException("Unknown Discovery type [" + discoveryType + "]");
        }

        if (discoveryType.equals("none") == false) {
            bind(UnicastHostsProvider.class).toInstance(hostsProvider);
        }
        bind(Discovery.class).to(discoveryClass).asEagerSingleton();
    }
}
