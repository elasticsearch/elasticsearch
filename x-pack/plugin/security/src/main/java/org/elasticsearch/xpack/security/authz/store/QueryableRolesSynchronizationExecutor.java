/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.Index;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.role.BulkRolesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.store.QueryableRolesProvider.QueryableRoles;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

public class QueryableRolesSynchronizationExecutor implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(QueryableRolesSynchronizationExecutor.class);

    public static final NodeFeature QUERYABLE_BUILT_IN_ROLES_FEATURE = new NodeFeature("security.queryable_built_in_roles");
    private static final String METADATA_QUERYABLE_BUILT_IN_ROLES = "queryable_built_in_roles";

    private static final SimpleBatchedExecutor<MarkBuiltinRolesAsSyncedTask, Map<String, String>> MARK_ROLES_AS_SYNCED_TASK_EXECUTOR =
        new SimpleBatchedExecutor<>() {
            @Override
            public Tuple<ClusterState, Map<String, String>> executeTask(MarkBuiltinRolesAsSyncedTask task, ClusterState clusterState) {
                return task.execute(clusterState);
            }

            @Override
            public void taskSucceeded(MarkBuiltinRolesAsSyncedTask task, Map<String, String> value) {
                task.success(value);
            }
        };

    private final MasterServiceTaskQueue<MarkBuiltinRolesAsSyncedTask> markRolesAsSyncedTaskQueue;

    private final QueryableRolesProvider builtinRolesProvider;
    private final NativeRolesStore nativeRolesStore;
    private final SecurityIndexManager securityIndex;
    private final Executor executor;
    private final FeatureService featureService;
    private final AtomicBoolean synchronizationInProgress = new AtomicBoolean(false);

    public QueryableRolesSynchronizationExecutor(
        ClusterService clusterService,
        FeatureService featureService,
        QueryableRolesProvider rolesProvider,
        NativeRolesStore nativeRolesStore,
        SecurityIndexManager securityIndex,
        ThreadPool threadPool
    ) {
        this.featureService = featureService;
        this.builtinRolesProvider = rolesProvider;
        this.nativeRolesStore = nativeRolesStore;
        this.securityIndex = securityIndex;
        this.executor = threadPool.generic();
        this.markRolesAsSyncedTaskQueue = clusterService.createTaskQueue(
            "mark-built-in-roles-as-synced-task-queue",
            Priority.LOW,
            MARK_ROLES_AS_SYNCED_TASK_EXECUTOR
        );
    }

    private boolean shouldSyncBuiltInRoles(ClusterState state) {
        if (nativeRolesStore.isEnabled() == false) {
            logger.info("Native role management is not enabled, skipping built-in roles synchronization");
            return false;
        }
        if (false == state.clusterRecovered()) {
            logger.info("Cluster state has not recovered yet, skipping built-in roles synchronization");
            return false;
        }
        if (false == state.nodes().isLocalNodeElectedMaster()) {
            logger.info("Local node is not the master, skipping built-in roles synchronization");
            return false;
        }
        if (state.nodes().getDataNodes().isEmpty()) {
            logger.info("No data nodes in the cluster, skipping built-in roles synchronization");
            return false;
        }
        // to keep things simple and avoid potential overwrites with an older version of built-in roles,
        // we only sync built-in roles if all nodes are on the same version
        if (isMixedVersionCluster(state.nodes())) {
            logger.info("Not all nodes are on the same version, skipping built-in roles synchronization");
            return false;
        }
        if (false == featureService.clusterHasFeature(state, QUERYABLE_BUILT_IN_ROLES_FEATURE)) {
            logger.info("Not all nodes support queryable built-in roles, skipping built-in roles synchronization");
            return false;
        }
        return true;
    }

    private static boolean isMixedVersionCluster(DiscoveryNodes nodes) {
        Version version = null;
        for (DiscoveryNode node : nodes) {
            if (version == null) {
                version = node.getVersion();
            } else if (version.equals(node.getVersion()) == false) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        if (false == shouldSyncBuiltInRoles(state)) {
            return;
        }

        final QueryableRoles roles = builtinRolesProvider.roles();
        final Map<String, String> indexedRolesVersions = readIndexedRolesVersion(state);
        if (roles.roleVersions().equals(indexedRolesVersions)) {
            logger.info("Security index already contains the latest built-in roles indexed, skipping synchronization");
            return;
        }

        if (synchronizationInProgress.compareAndSet(false, true)) {
            executor.execute(() -> syncBuiltinRoles(indexedRolesVersions, roles, ActionListener.wrap(v -> {
                logger.info("Successfully synced built-in roles to security index");
                synchronizationInProgress.set(false);
            }, e -> {
                if (false == e instanceof UnavailableShardsException) {
                    logger.warn("Failed to sync built-in roles to security index", e);
                }
                synchronizationInProgress.set(false);
            })));
        }
    }

    private void syncBuiltinRoles(Map<String, String> indexedRolesVersions, QueryableRoles roles, ActionListener<Void> listener) {
        // This will create .security index if it does not exist and execute all migrations.
        securityIndex.prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            final SecurityIndexManager frozenSecurityIndex = securityIndex.defensiveCopy();
            if (frozenSecurityIndex.isAvailable(PRIMARY_SHARDS) == false) {
                listener.onFailure(frozenSecurityIndex.getUnavailableReason(PRIMARY_SHARDS));
            } else {
                indexRoles(roles.roleDescriptors().values(), frozenSecurityIndex, ActionListener.wrap(onResponse -> {
                    Set<String> rolesToDelete = indexedRolesVersions == null
                        ? Set.of()
                        : Sets.difference(indexedRolesVersions.keySet(), roles.roleVersions().keySet());
                    if (false == rolesToDelete.isEmpty()) {
                        deleteRoles(rolesToDelete, roles.roleVersions(), frozenSecurityIndex, indexedRolesVersions, listener);
                    } else {
                        markRolesAsSynced(frozenSecurityIndex.getConcreteIndexName(), indexedRolesVersions, roles.roleVersions(), listener);
                    }
                }, listener::onFailure));
            }
        });

    }

    private void deleteRoles(
        Set<String> rolesToDelete,
        Map<String, String> roleVersions,
        SecurityIndexManager frozenSecurityIndex,
        Map<String, String> indexedRolesVersions,
        ActionListener<Void> listener
    ) {
        nativeRolesStore.deleteRoles(
            frozenSecurityIndex,
            rolesToDelete,
            WriteRequest.RefreshPolicy.IMMEDIATE,
            false,
            ActionListener.wrap(deleteResponse -> {
                if (deleteResponse.getItems().stream().anyMatch(BulkRolesResponse.Item::isFailed)) {
                    listener.onFailure(
                        new ElasticsearchStatusException("Automatic deletion of built-in roles failed", RestStatus.INTERNAL_SERVER_ERROR)
                    );
                } else {
                    markRolesAsSynced(frozenSecurityIndex.getConcreteIndexName(), indexedRolesVersions, roleVersions, listener);
                }

            }, listener::onFailure)
        );
    }

    private void indexRoles(
        Collection<RoleDescriptor> roleDescriptors,
        SecurityIndexManager frozenSecurityIndex,
        ActionListener<Void> listener
    ) {
        nativeRolesStore.putRoles(
            frozenSecurityIndex,
            WriteRequest.RefreshPolicy.IMMEDIATE,
            roleDescriptors,
            false,
            ActionListener.wrap(response -> {
                if (response.getItems().stream().anyMatch(BulkRolesResponse.Item::isFailed)) {
                    logger.warn("Automatic indexing of built-in roles failed: {}", response);
                    listener.onFailure(new ElasticsearchException("Automatic indexing of built-in roles failed"));
                } else {
                    listener.onResponse(null);
                }
            }, listener::onFailure)
        );
    }

    private void markRolesAsSynced(
        String concreteSecurityIndexName,
        Map<String, String> expectedRolesVersion,
        Map<String, String> newRolesVersion,
        ActionListener<Void> listener
    ) {
        markRolesAsSyncedTaskQueue.submitTask(
            "mark built-in roles as synced task",
            new MarkBuiltinRolesAsSyncedTask(ActionListener.wrap(response -> {
                if (newRolesVersion.equals(response) == false) {
                    listener.onFailure(new IllegalStateException("Failed to mark built-in roles as synced"));
                } else {
                    listener.onResponse(null);
                }
            }, listener::onFailure), concreteSecurityIndexName, expectedRolesVersion, newRolesVersion),
            null
        );
    }

    private Map<String, String> readIndexedRolesVersion(ClusterState state) {
        final IndexMetadata indexMetadata = resolveSecurityIndexMetadata(state.metadata());
        if (indexMetadata == null) {
            return null;
        }
        return indexMetadata.getCustomData(METADATA_QUERYABLE_BUILT_IN_ROLES);
    }

    private static IndexMetadata resolveSecurityIndexMetadata(final Metadata metadata) {
        final Index index = resolveConcreteSecurityIndex(metadata);
        if (index != null) {
            return metadata.getIndexSafe(index);
        }
        return null;
    }

    private static Index resolveConcreteSecurityIndex(final Metadata metadata) {
        final IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(SECURITY_MAIN_ALIAS);
        if (indexAbstraction != null) {
            final List<Index> indices = indexAbstraction.getIndices();
            if (indexAbstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX && indices.size() > 1) {
                throw new IllegalStateException("Alias [" + SECURITY_MAIN_ALIAS + "] points to more than one index: " + indices);
            }
            return indices.get(0);
        }
        return null;
    }

    static class MarkBuiltinRolesAsSyncedTask implements ClusterStateTaskListener {

        private final ActionListener<Map<String, String>> listener;
        private final String index;
        @Nullable
        private final Map<String, String> expected;
        @Nullable
        private final Map<String, String> value;

        MarkBuiltinRolesAsSyncedTask(
            ActionListener<Map<String, String>> listener,
            String index,
            @Nullable Map<String, String> expected,
            @Nullable Map<String, String> value
        ) {
            this.listener = listener;
            this.index = index;
            this.expected = expected;
            this.value = value;
        }

        Tuple<ClusterState, Map<String, String>> execute(ClusterState state) {
            IndexMetadata indexMetadata = state.metadata().index(index);
            Map<String, String> existingValue = indexMetadata.getCustomData(METADATA_QUERYABLE_BUILT_IN_ROLES);
            if (Objects.equals(expected, existingValue)) {
                IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
                if (value != null) {
                    indexMetadataBuilder.putCustom(METADATA_QUERYABLE_BUILT_IN_ROLES, value);
                } else {
                    indexMetadataBuilder.removeCustom(METADATA_QUERYABLE_BUILT_IN_ROLES);
                }
                indexMetadataBuilder.version(indexMetadataBuilder.version() + 1);
                ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(state.metadata().indices());
                builder.put(index, indexMetadataBuilder.build());
                return new Tuple<>(
                    ClusterState.builder(state).metadata(Metadata.builder(state.metadata()).indices(builder.build()).build()).build(),
                    value
                );
            } else {
                // returns existing value when expectation is not met
                return new Tuple<>(state, existingValue);
            }
        }

        void success(Map<String, String> value) {
            listener.onResponse(value);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

}
