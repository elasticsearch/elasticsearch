/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;

/**
 * Plugin for Searchable Snapshots feature
 */
public class SearchableSnapshots extends Plugin implements IndexStorePlugin, RepositoryPlugin, EnginePlugin {

    private final SetOnce<RepositoriesService> repositoriesService;

    public SearchableSnapshots() {
        this.repositoriesService = new SetOnce<>();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(SearchableSnapshotRepository.SNAPSHOT_REPOSITORY_SETTING,
            SearchableSnapshotRepository.SNAPSHOT_SNAPSHOT_NAME_SETTING,
            SearchableSnapshotRepository.SNAPSHOT_SNAPSHOT_ID_SETTING,
            SearchableSnapshotRepository.SNAPSHOT_INDEX_ID_SETTING);
    }

    @Override
    public void onRepositoriesModule(RepositoriesModule repositoriesModule) {
        repositoriesService.set(repositoriesModule.getRepositoryService()); // should we use some SPI mechanism?
    }

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Map.of(SearchableSnapshotRepository.SNAPSHOT_DIRECTORY_FACTORY_KEY,
            SearchableSnapshotRepository.newDirectoryFactory(repositoriesService::get));
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (SearchableSnapshotRepository.SNAPSHOT_DIRECTORY_FACTORY_KEY.equals(INDEX_STORE_TYPE_SETTING.get(indexSettings.getSettings()))) {
            return Optional.of(engineConfig -> new ReadOnlyEngine(engineConfig, null, new TranslogStats(), false, Function.identity()));
        }
        return Optional.empty();
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                           ClusterService clusterService) {
        return Collections.singletonMap(SearchableSnapshotRepository.TYPE, SearchableSnapshotRepository.getRepositoryFactory());
    }
}

