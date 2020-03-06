/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheAction;
import org.elasticsearch.xpack.searchablesnapshots.action.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.TransportClearSearchableSnapshotsCacheAction;
import org.elasticsearch.xpack.searchablesnapshots.action.TransportMountSearchableSnapshotAction;
import org.elasticsearch.xpack.searchablesnapshots.action.TransportSearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheDirectory;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.rest.RestClearSearchableSnapshotsCacheAction;
import org.elasticsearch.xpack.searchablesnapshots.rest.RestMountSearchableSnapshotAction;
import org.elasticsearch.xpack.searchablesnapshots.rest.RestSearchableSnapshotsStatsAction;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotRepository.SNAPSHOT_CACHE_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotRepository.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotRepository.SNAPSHOT_SNAPSHOT_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotRepository.SNAPSHOT_SNAPSHOT_NAME_SETTING;

/**
 * Plugin for Searchable Snapshots feature
 */
public class SearchableSnapshots extends Plugin implements IndexStorePlugin, RepositoryPlugin, EnginePlugin, ActionPlugin {

    private final SetOnce<RepositoriesService> repositoriesService;
    private final SetOnce<CacheService> cacheService;
    private final Settings settings;

    public SearchableSnapshots(final Settings settings) {
        this.repositoriesService = new SetOnce<>();
        this.cacheService = new SetOnce<>();
        this.settings = settings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(SearchableSnapshotRepository.SNAPSHOT_REPOSITORY_SETTING,
            SNAPSHOT_SNAPSHOT_NAME_SETTING,
            SNAPSHOT_SNAPSHOT_ID_SETTING,
            SNAPSHOT_INDEX_ID_SETTING,
            SNAPSHOT_CACHE_ENABLED_SETTING,
            CacheService.SNAPSHOT_CACHE_SIZE_SETTING,
            CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING
        );
    }

    @Override
    public Collection<Object> createComponents(
        final Client client,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ResourceWatcherService resourceWatcherService,
        final ScriptService scriptService,
        final NamedXContentRegistry xContentRegistry,
        final Environment environment,
        final NodeEnvironment nodeEnvironment,
        final NamedWriteableRegistry registry,
        final IndexNameExpressionResolver resolver) {

        final CacheService cacheService = new CacheService(settings);
        this.cacheService.set(cacheService);
        return List.of(cacheService);
    }

    @Override
    public void onRepositoriesModule(RepositoriesModule repositoriesModule) {
        // TODO NORELEASE should we use some SPI mechanism? The only reason we are a RepositoriesPlugin is because of this :/
        repositoriesService.set(repositoriesModule.getRepositoryService());
    }

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Map.of(SearchableSnapshotRepository.SNAPSHOT_DIRECTORY_FACTORY_KEY,
            new SearchableSnapshotsDirectoryFactory(repositoriesService::get, cacheService::get, System::nanoTime));
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (SearchableSnapshotRepository.SNAPSHOT_DIRECTORY_FACTORY_KEY.equals(INDEX_STORE_TYPE_SETTING.get(indexSettings.getSettings()))
            && indexSettings.getSettings().getAsBoolean("index.frozen", false) == false) {
            return Optional.of(engineConfig -> new ReadOnlyEngine(engineConfig, null, new TranslogStats(), false, Function.identity()));
        }
        return Optional.empty();
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(SearchableSnapshotsStatsAction.INSTANCE, TransportSearchableSnapshotsStatsAction.class),
            new ActionHandler<>(ClearSearchableSnapshotsCacheAction.INSTANCE, TransportClearSearchableSnapshotsCacheAction.class),
            new ActionHandler<>(MountSearchableSnapshotAction.INSTANCE, TransportMountSearchableSnapshotAction.class)
        );
    }

    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return List.of(
            new RestSearchableSnapshotsStatsAction(),
            new RestClearSearchableSnapshotsCacheAction(),
            new RestMountSearchableSnapshotAction()
        );
    }

    private static class SearchableSnapshotsDirectoryFactory implements DirectoryFactory {
        private final Supplier<RepositoriesService> repositoriesService;
        private final Supplier<CacheService> cacheService;
        private final LongSupplier currentTimeNanosSupplier;

        SearchableSnapshotsDirectoryFactory(Supplier<RepositoriesService> repositoriesService,
                                            Supplier<CacheService> cacheService,
                                            LongSupplier currentTimeNanosSupplier) {
            this.repositoriesService = repositoriesService;
            this.cacheService = cacheService;
            this.currentTimeNanosSupplier = currentTimeNanosSupplier;
        }

        @Override
        public Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
            final RepositoriesService repositories = repositoriesService.get();
            assert repositories != null;
            final CacheService cache = cacheService.get();
            assert cache != null;

            final Repository repository = repositories.repository(
                SearchableSnapshotRepository.SNAPSHOT_REPOSITORY_SETTING.get(indexSettings.getSettings()));
            if (repository instanceof BlobStoreRepository == false) {
                throw new IllegalArgumentException("Repository [" + repository + "] is not searchable");
            }
            final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;

            IndexId indexId = new IndexId(indexSettings.getIndex().getName(), SNAPSHOT_INDEX_ID_SETTING.get(indexSettings.getSettings()));
            BlobContainer blobContainer = blobStoreRepository.shardContainer(indexId, shardPath.getShardId().id());

            SnapshotId snapshotId = new SnapshotId(SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings.getSettings()),
                SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings.getSettings()));
            BlobStoreIndexShardSnapshot snapshot = blobStoreRepository.loadShardSnapshot(blobContainer, snapshotId);

            Directory directory = new SearchableSnapshotDirectory(snapshot, blobContainer);
            if (SNAPSHOT_CACHE_ENABLED_SETTING.get(indexSettings.getSettings())) {
                final Path cacheDir = shardPath.getDataPath().resolve("snapshots").resolve(snapshotId.getUUID());
                directory = new CacheDirectory(directory, cache, cacheDir, snapshotId, indexId, shardPath.getShardId(),
                    currentTimeNanosSupplier);
            }
            directory = new InMemoryNoOpCommitDirectory(directory);

            final IndexWriterConfig indexWriterConfig = new IndexWriterConfig(null)
                .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
                .setMergePolicy(NoMergePolicy.INSTANCE);

            try (IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig)) {
                final Map<String, String> userData = new HashMap<>();
                indexWriter.getLiveCommitData().forEach(e -> userData.put(e.getKey(), e.getValue()));

                final String translogUUID = Translog.createEmptyTranslog(shardPath.resolveTranslog(),
                    Long.parseLong(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)),
                    shardPath.getShardId(), 0L);

                userData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
                indexWriter.setLiveCommitData(userData.entrySet());
                indexWriter.commit();
            }

            return directory;
        }
    }

}

