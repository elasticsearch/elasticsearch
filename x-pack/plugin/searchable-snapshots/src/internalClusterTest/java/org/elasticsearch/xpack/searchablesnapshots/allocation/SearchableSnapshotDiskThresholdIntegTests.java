/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.DiskUsageIntegTestCase;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import org.elasticsearch.xpack.searchablesnapshots.LocalStateSearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.index.store.Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.license.LicenseService.SELF_GENERATED_LICENSE_TYPE;
import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage.FULL_COPY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchableSnapshotDiskThresholdIntegTests extends DiskUsageIntegTestCase {

    private static final long WATERMARK_BYTES = new ByteSizeValue(10, ByteSizeUnit.KB).getBytes();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), WATERMARK_BYTES + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), WATERMARK_BYTES + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "0b")
            .put(SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            // we want to control the refresh of cluster info updates
            .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.getKey(), "60m")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(LocalStateSearchableSnapshots.class, MockRepository.Plugin.class))
            .toList();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    private int createIndices() throws InterruptedException {
        final int nbIndices = randomIntBetween(1, 5);
        final CountDownLatch latch = new CountDownLatch(nbIndices);

        for (int i = 0; i < nbIndices; i++) {
            final String index = "index-" + i;
            var thread = new Thread(() -> {
                try {
                    createIndex(
                        index,
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                            .put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
                            .put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
                            .put(DataTier.TIER_PREFERENCE_SETTING.getKey(), DataTier.DATA_HOT)
                            // Disable merges. A merge can cause discrepancy between the size we detect and the size in the snapshot,
                            // which could make room for more shards.
                            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                            .build()
                    );
                    int nbDocs = 100;
                    try (BackgroundIndexer indexer = new BackgroundIndexer(index, client(), nbDocs)) {
                        while (true) {
                            waitForDocs(nbDocs, indexer);
                            indexer.assertNoFailures();
                            assertNoFailures(
                                client().admin().indices().prepareForceMerge().setFlush(true).setIndices(index).setMaxNumSegments(1).get()
                            );
                            Map<String, Long> storeSize = sizeOfShardsStores(index);
                            if (storeSize.get(index) > WATERMARK_BYTES) {
                                break;
                            }
                            int moreDocs = scaledRandomIntBetween(100, 1_000);
                            indexer.continueIndexing(moreDocs);
                            nbDocs += moreDocs;
                        }
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                } finally {
                    latch.countDown();
                }
            });
            thread.start();
        }
        latch.await();
        return nbIndices;
    }

    private void createSnapshot(String repository, String snapshot, int nbIndices) {
        var snapshotInfo = client().admin()
            .cluster()
            .prepareCreateSnapshot(repository, snapshot)
            .setIndices("index-*")
            .setIncludeGlobalState(false)
            .setWaitForCompletion(true)
            .get()
            .getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), equalTo(nbIndices));
        assertThat(snapshotInfo.failedShards(), equalTo(0));
    }

    private void mountIndices(Collection<String> indices, String prefix, String repositoryName, String snapshotName, Storage storage)
        throws InterruptedException {
        CountDownLatch mountLatch = new CountDownLatch(indices.size());
        logger.info("--> mounting [{}] indices with [{}] prefix", indices.size(), prefix);
        for (String index : indices) {
            logger.info("Mounting index {}", index);
            client().execute(
                MountSearchableSnapshotAction.INSTANCE,
                new MountSearchableSnapshotRequest(
                    prefix + index,
                    repositoryName,
                    snapshotName,
                    index,
                    Settings.EMPTY,
                    Strings.EMPTY_ARRAY,
                    false,
                    storage
                ),
                ActionListener.wrap(response -> mountLatch.countDown(), e -> mountLatch.countDown())
            );
        }
        mountLatch.await();
    }

    public void testHighWatermarkCanNotBeExceededOnColdNode() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startNode(onlyRole(DATA_HOT_NODE_ROLE));

        final int nbIndices = createIndices();

        final String repositoryName = "repository";
        repository(repositoryName, FsRepository.TYPE);

        final String snapshot = "snapshot";
        createSnapshot(repositoryName, snapshot, nbIndices);

        final Map<String, Long> indicesStoresSizes = sizeOfShardsStores("index-*");
        assertAcked(client().admin().indices().prepareDelete("index-*"));

        // The test completes reliably successfully only when we do a full copy, we can overcommit on SHARED_CACHE
        final Storage storage = FULL_COPY;
        logger.info("--> using storage [{}]", storage);

        final Settings.Builder otherDataNodeSettings = Settings.builder();
        if (storage == FULL_COPY) {
            otherDataNodeSettings.put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_COLD_NODE_ROLE.roleName());
        } else {
            otherDataNodeSettings.put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
                .put(
                    FrozenCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                    ByteSizeValue.ofBytes(Math.min(indicesStoresSizes.values().stream().mapToLong(value -> value).sum(), 5 * 1024L * 1024L))
                );
        }
        final String otherDataNode = internalCluster().startNode(otherDataNodeSettings.build());
        ensureStableCluster(3);

        final String otherDataNodeId = internalCluster().getInstance(NodeEnvironment.class, otherDataNode).nodeId();
        logger.info("--> reducing disk size of node [{}/{}] so that all shards can fit on the node", otherDataNode, otherDataNodeId);
        final long totalSpace = indicesStoresSizes.values().stream().mapToLong(size -> size).sum() + WATERMARK_BYTES + 1024L;
        getTestFileStore(otherDataNode).setTotalSpace(totalSpace);

        logger.info("--> refreshing cluster info");
        final var masterInfoService = (InternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        ClusterInfoServiceUtils.refresh(masterInfoService);

        assertThat(
            masterInfoService.getClusterInfo().getNodeMostAvailableDiskUsages().get(otherDataNodeId).getTotalBytes(),
            equalTo(totalSpace)
        );

        mountIndices(indicesStoresSizes.keySet(), "mounted-", repositoryName, snapshot, storage);

        // The cold/frozen data node has enough disk space to hold all the shards
        assertBusy(() -> {
            var state = client().admin().cluster().prepareState().setRoutingTable(true).get().getState();
            assertThat(
                state.routingTable()
                    .allShards()
                    .stream()
                    .filter(shardRouting -> state.metadata().index(shardRouting.shardId().getIndex()).isSearchableSnapshot())
                    .allMatch(
                        shardRouting -> shardRouting.state() == ShardRoutingState.STARTED
                            && otherDataNodeId.equals(shardRouting.currentNodeId())
                    ),
                equalTo(true)
            );
        });

        mountIndices(indicesStoresSizes.keySet(), "extra-", repositoryName, snapshot, storage);

        assertBusy(() -> {
            var state = client().admin().cluster().prepareState().setRoutingTable(true).get().getState();
            assertThat(
                state.routingTable()
                    .allShards()
                    .stream()
                    .filter(
                        shardRouting -> shardRouting.shardId().getIndexName().startsWith("extra-")
                            && state.metadata().index(shardRouting.shardId().getIndex()).isSearchableSnapshot()
                    )
                    .noneMatch(
                        shardRouting -> shardRouting.state() == ShardRoutingState.STARTED
                            && otherDataNodeId.equals(shardRouting.currentNodeId())
                    ),
                equalTo(true)
            );
        });
    }

    public void testHighWatermarkCanBeExceededOnColdNode() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startNode(onlyRole(DATA_HOT_NODE_ROLE));

        int nbIndices = createIndices();

        String repositoryName = "repository";
        repository(repositoryName, "mock");

        String snapshotName = "snapshot";
        createSnapshot(repositoryName, snapshotName, nbIndices);

        Map<String, Long> indicesStoresSizes = sizeOfShardsStores("index-*");
        assertAcked(client().admin().indices().prepareDelete("index-*"));

        String otherDataNode = internalCluster().startNode(
            Settings.builder().put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_COLD_NODE_ROLE.roleName()).build()
        );
        ensureStableCluster(3);

        String otherDataNodeId = client().admin().cluster().prepareState().get().getState().nodes().resolveNode(otherDataNode).getId();
        logger.info(
            "--> reducing disk size of node [{}/{}] so that all shards except one can fit on the node",
            otherDataNode,
            otherDataNodeId
        );
        String indexToSkip = randomFrom(indicesStoresSizes.keySet());
        Map<String, Long> indicesToBeMounted = indicesStoresSizes.entrySet()
            .stream()
            .filter(e -> e.getKey().equals(indexToSkip) == false)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        long totalSpace = indicesToBeMounted.values().stream().mapToLong(e -> e).sum() + WATERMARK_BYTES + 1024L;
        getTestFileStore(otherDataNode).setTotalSpace(totalSpace);

        logger.info("--> refreshing cluster info");
        InternalClusterInfoService masterInfoService = (InternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(
            ClusterInfoService.class
        );
        ClusterInfoServiceUtils.refresh(masterInfoService);
        assertThat(
            masterInfoService.getClusterInfo().getNodeMostAvailableDiskUsages().get(otherDataNodeId).getTotalBytes(),
            equalTo(totalSpace)
        );

        var mockRepository = (MockRepository) internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class)
            .repository(repositoryName);
        // Prevent searchable snapshot shards from quickly jumping from INITIALIZED to STARTED
        mockRepository.setBlockOnceOnReadSnapshotInfoIfAlreadyBlocked();

        String prefix = "mounted-";
        mountIndices(indicesToBeMounted.keySet(), prefix, repositoryName, snapshotName, FULL_COPY);
        assertBusy(() -> {
            var state = client().admin().cluster().prepareState().setRoutingTable(true).get().getState();
            assertThat(
                state.routingTable()
                    .allShards()
                    .stream()
                    .filter(s -> indicesToBeMounted.containsKey(s.shardId().getIndexName().replace(prefix, "")))
                    .filter(s -> state.metadata().index(s.shardId().getIndex()).isSearchableSnapshot())
                    .filter(s -> otherDataNodeId.equals(s.currentNodeId()))
                    .filter(s -> s.state() == ShardRoutingState.STARTED)
                    .count(),
                equalTo((long) indicesToBeMounted.size())
            );
        });

        mountIndices(List.of(indexToSkip), prefix, repositoryName, snapshotName, FULL_COPY);
        assertBusy(() -> {
            var state = client().admin().cluster().prepareState().setRoutingTable(true).get().getState();
            assertThat(
                state.routingTable()
                    .allShards()
                    .stream()
                    .filter(s -> indexToSkip.equals(s.shardId().getIndexName().replace(prefix, "")))
                    .filter(s -> state.metadata().index(s.shardId().getIndex()).isSearchableSnapshot())
                    .filter(s -> otherDataNodeId.equals(s.currentNodeId()))
                    .filter(s -> s.state() == ShardRoutingState.STARTED)
                    .count(),
                equalTo(0L)
            );
            assertEquals(ClusterHealthStatus.RED, client().admin().cluster().health(new ClusterHealthRequest()).actionGet().getStatus());
        });
    }

    private void repository(String name, String type) {
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(name)
                .setType(type)
                .setSettings(Settings.builder().put("location", randomRepoPath()).build())
        );
    }

    private static Map<String, Long> sizeOfShardsStores(String indexPattern) {
        return Arrays.stream(client().admin().indices().prepareStats(indexPattern).clear().setStore(true).get().getShards())
            .collect(
                Collectors.toUnmodifiableMap(s -> s.getShardRouting().getIndexName(), s -> s.getStats().getStore().sizeInBytes(), Long::sum)
            );
    }
}
