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

package org.elasticsearch.plugin.discovery.azure.classic;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeService;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeServiceImpl;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.azure.classic.AzureUnicastHostsProvider;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class AzureDiscoveryPlugin extends Plugin implements DiscoveryPlugin {

    public static final String AZURE = "azure";
    protected final Settings settings;
    private static final Logger logger = Loggers.getLogger(AzureDiscoveryPlugin.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public AzureDiscoveryPlugin(Settings settings) {
        this.settings = settings;
        deprecationLogger.deprecated("azure classic discovery plugin is deprecated. Use azure arm discovery plugin instead");
        logger.trace("starting azure classic discovery plugin...");
    }

    // overrideable for tests
    protected AzureComputeService createComputeService() {
        return new AzureComputeServiceImpl(settings);
    }

    @Override
    public Map<String, Supplier<UnicastHostsProvider>> getZenHostsProviders(TransportService transportService,
                                                                            NetworkService networkService) {
        return Collections.singletonMap(AZURE,
            () -> createUnicastHostsProvider(settings, createComputeService(), transportService, networkService));
    }

    // Used for testing
    protected AzureUnicastHostsProvider createUnicastHostsProvider(final Settings settings,
                                                                   final AzureComputeService azureComputeService,
                                                                   final TransportService transportService,
                                                                   final NetworkService networkService) {
        return new AzureUnicastHostsProvider(settings, azureComputeService, transportService, networkService);
    }


    public static PluginSettings getPluginSettings(Settings settings) {
        return new PluginSettings() {
            @Override
            public List<Setting<?>> getDeclaredSettings() {
                return Arrays.asList(AzureComputeService.Discovery.REFRESH_SETTING,
                    AzureComputeService.Management.KEYSTORE_PASSWORD_SETTING,
                    AzureComputeService.Management.KEYSTORE_PATH_SETTING,
                    AzureComputeService.Management.KEYSTORE_TYPE_SETTING,
                    AzureComputeService.Management.SUBSCRIPTION_ID_SETTING,
                    AzureComputeService.Management.SERVICE_NAME_SETTING,
                    AzureComputeService.Discovery.HOST_TYPE_SETTING,
                    AzureComputeService.Discovery.DEPLOYMENT_NAME_SETTING,
                    AzureComputeService.Discovery.DEPLOYMENT_SLOT_SETTING,
                    AzureComputeService.Discovery.ENDPOINT_NAME_SETTING);
            }
        };
    }
}
