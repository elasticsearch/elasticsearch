/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.repositories.stats.azure;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.repositories.stats.AbstractRepositoriesStatsAPIRestTestCase;

import java.util.List;

public class AzureRepositoriesStatsIT extends AbstractRepositoriesStatsAPIRestTestCase {

    @Override
    protected String repositoryType() {
        return "azure";
    }

    @Override
    protected String repositoryLocation() {
        return getProperty("test.azure.container") + "/" + getProperty("test.azure.base_path") + "/";
    }

    @Override
    protected Settings repositorySettings() {
        final String container = getProperty("test.azure.container");

        final String basePath = getProperty("test.azure.base_path");

        return Settings.builder().put("client", "repositories_stats").put("container", container).put("base_path", basePath).build();
    }

    @Override
    protected Settings updatedRepositorySettings() {
        return Settings.builder().put(repositorySettings()).put("azure.client.repositories_stats.max_retries", 5).build();
    }

    @Override
    protected List<String> readCounterKeys() {
        return List.of("GET", "HEAD", "LIST");
    }

    @Override
    protected List<String> writeCounterKeys() {
        return List.of("PUT");
    }
}
