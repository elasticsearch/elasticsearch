/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import com.carrotsearch.hppc.predicates.ObjectPredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.CopyOnWriteHashMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ccr.AutoFollowStats.AutoFollowedCluster;

/**
 * A component that runs only on the elected master node and follows leader indices automatically
 * if they match with a auto follow pattern that is defined in {@link AutoFollowMetadata}.
 */
public class AutoFollowCoordinator implements ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(AutoFollowCoordinator.class);
    private static final int MAX_AUTO_FOLLOW_ERRORS = 256;

    private final Client client;
    private final ClusterService clusterService;
    private final CcrLicenseChecker ccrLicenseChecker;
    private final LongSupplier relativeMillisTimeProvider;

    private volatile Map<String, AutoFollower> autoFollowers = Collections.emptyMap();

    // The following fields are read and updated under a lock:
    private long numberOfSuccessfulIndicesAutoFollowed = 0;
    private long numberOfFailedIndicesAutoFollowed = 0;
    private long numberOfFailedRemoteClusterStateRequests = 0;
    private final LinkedHashMap<String, ElasticsearchException> recentAutoFollowErrors;

    public AutoFollowCoordinator(
            Client client,
            ClusterService clusterService,
            CcrLicenseChecker ccrLicenseChecker,
            LongSupplier relativeMillisTimeProvider) {

        this.client = client;
        this.clusterService = clusterService;
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker, "ccrLicenseChecker");
        this.relativeMillisTimeProvider = relativeMillisTimeProvider;
        clusterService.addListener(this);
        this.recentAutoFollowErrors = new LinkedHashMap<String, ElasticsearchException>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<String, ElasticsearchException> eldest) {
                return size() > MAX_AUTO_FOLLOW_ERRORS;
            }
        };
    }

    public synchronized AutoFollowStats getStats() {
        final Map<String, AutoFollower> autoFollowers = this.autoFollowers;
        final TreeMap<String, AutoFollowedCluster> timesSinceLastAutoFollowPerRemoteCluster = new TreeMap<>();
        for (Map.Entry<String, AutoFollower> entry : autoFollowers.entrySet()) {
            long lastAutoFollowTimeInMillis = entry.getValue().lastAutoFollowTimeInMillis;
            long lastSeenMetadataVersion = entry.getValue().metadataVersion;
            if (lastAutoFollowTimeInMillis != -1) {
                long timeSinceLastCheckInMillis = relativeMillisTimeProvider.getAsLong() - lastAutoFollowTimeInMillis;
                timesSinceLastAutoFollowPerRemoteCluster.put(entry.getKey(),
                    new AutoFollowedCluster(timeSinceLastCheckInMillis, lastSeenMetadataVersion));
            } else {
                timesSinceLastAutoFollowPerRemoteCluster.put(entry.getKey(), new AutoFollowedCluster(-1L, lastSeenMetadataVersion));
            }
        }

        return new AutoFollowStats(
            numberOfFailedIndicesAutoFollowed,
            numberOfFailedRemoteClusterStateRequests,
            numberOfSuccessfulIndicesAutoFollowed,
            new TreeMap<>(recentAutoFollowErrors),
            timesSinceLastAutoFollowPerRemoteCluster
        );
    }

    synchronized void updateStats(List<AutoFollowResult> results) {
        for (AutoFollowResult result : results) {
            if (result.clusterStateFetchException != null) {
                recentAutoFollowErrors.put(result.autoFollowPatternName,
                    new ElasticsearchException(result.clusterStateFetchException));
                numberOfFailedRemoteClusterStateRequests++;
                LOGGER.warn(new ParameterizedMessage("failure occurred while fetching cluster state for auto follow pattern [{}]",
                    result.autoFollowPatternName), result.clusterStateFetchException);
            } else {
                for (Map.Entry<Index, Exception> entry : result.autoFollowExecutionResults.entrySet()) {
                    if (entry.getValue() != null) {
                        numberOfFailedIndicesAutoFollowed++;
                        recentAutoFollowErrors.put(result.autoFollowPatternName + ":" + entry.getKey().getName(),
                            ExceptionsHelper.convertToElastic(entry.getValue()));
                        LOGGER.warn(new ParameterizedMessage("failure occurred while auto following index [{}] for auto follow " +
                            "pattern [{}]", entry.getKey(), result.autoFollowPatternName), entry.getValue());
                    } else {
                        numberOfSuccessfulIndicesAutoFollowed++;
                    }
                }
            }

        }
    }

    void updateAutoFollowers(ClusterState followerClusterState) {
        AutoFollowMetadata autoFollowMetadata = followerClusterState.getMetaData().custom(AutoFollowMetadata.TYPE);
        if (autoFollowMetadata == null) {
            return;
        }

        if (ccrLicenseChecker.isCcrAllowed() == false) {
            // TODO: set non-compliant status on auto-follow coordination that can be viewed via a stats API
            LOGGER.warn("skipping auto-follower coordination", LicenseUtils.newComplianceException("ccr"));
            return;
        }

        final CopyOnWriteHashMap<String, AutoFollower> autoFollowers = CopyOnWriteHashMap.copyOf(this.autoFollowers);
        Set<String> newRemoteClusters = autoFollowMetadata.getPatterns().entrySet().stream()
            .map(entry -> entry.getValue().getRemoteCluster())
            .filter(remoteCluster -> autoFollowers.containsKey(remoteCluster) == false)
            .collect(Collectors.toSet());

        Map<String, AutoFollower> newAutoFollowers = new HashMap<>(newRemoteClusters.size());
        for (String remoteCluster : newRemoteClusters) {
            AutoFollower autoFollower =
                new AutoFollower(remoteCluster, this::updateStats, clusterService::state, relativeMillisTimeProvider) {

                @Override
                void getRemoteClusterState(final String remoteCluster,
                                           final long metadataVersion,
                                           final BiConsumer<ClusterStateResponse, Exception> handler) {
                    final ClusterStateRequest request = new ClusterStateRequest();
                    request.clear();
                    request.metaData(true);
                    request.routingTable(true);
                    request.waitForMetaDataVersion(metadataVersion);
                    // TODO: set non-compliant status on auto-follow coordination that can be viewed via a stats API
                    ccrLicenseChecker.checkRemoteClusterLicenseAndFetchClusterState(
                        client,
                        remoteCluster,
                        request,
                        e -> handler.accept(null, e),
                        remoteClusterStateResponse -> handler.accept(remoteClusterStateResponse, null));
                }

                @Override
                void createAndFollow(Map<String, String> headers,
                                     PutFollowAction.Request request,
                                     Runnable successHandler,
                                     Consumer<Exception> failureHandler) {
                    Client followerClient = CcrLicenseChecker.wrapClient(client, headers);
                    followerClient.execute(
                        PutFollowAction.INSTANCE,
                        request,
                        ActionListener.wrap(r -> successHandler.run(), failureHandler)
                    );
                }

                @Override
                void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction,
                                              Consumer<Exception> handler) {
                    clusterService.submitStateUpdateTask("update_auto_follow_metadata", new ClusterStateUpdateTask() {

                        @Override
                        public ClusterState execute(ClusterState currentState) throws Exception {
                            return updateFunction.apply(currentState);
                        }

                        @Override
                        public void onFailure(String source, Exception e) {
                            handler.accept(e);
                        }

                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                            handler.accept(null);
                        }
                    });
                }

            };
            newAutoFollowers.put(remoteCluster, autoFollower);
            autoFollower.start();
        }

        List<String> removedRemoteClusters = new ArrayList<>();
        for (String remoteCluster : autoFollowers.keySet()) {
            boolean exist = autoFollowMetadata.getPatterns().values().stream()
                .anyMatch(pattern -> pattern.getRemoteCluster().equals(remoteCluster));
            if (exist == false) {
                removedRemoteClusters.add(remoteCluster);
            }
        }
        this.autoFollowers = autoFollowers
            .copyAndPutAll(newAutoFollowers)
            .copyAndRemoveAll(removedRemoteClusters);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            updateAutoFollowers(event.state());
        }
    }

    /**
     * Each auto follower independently monitors a remote cluster for new leader indices that should be auto followed.
     * The reason that this should happen independently, is that when auto followers start to make use of cluster state
     * API's 'wait_for_metadata_version' feature, it may take sometime before a remote cluster responds with a new
     * cluster state or times out. Other auto follow patterns for different remote clusters are then forced to wait,
     * which can cause new follower indices to unnecessarily start with a large backlog of operations that need to be
     * replicated.
     */
    abstract static class AutoFollower {

        private final String remoteCluster;
        private final Consumer<List<AutoFollowResult>> statsUpdater;
        private final Supplier<ClusterState> followerClusterStateSupplier;
        private final LongSupplier relativeTimeProvider;

        private volatile long lastAutoFollowTimeInMillis = -1;
        private volatile long metadataVersion = 0;
        private volatile CountDown autoFollowPatternsCountDown;
        private volatile AtomicArray<AutoFollowResult> autoFollowResults;

        AutoFollower(final String remoteCluster,
                     final Consumer<List<AutoFollowResult>> statsUpdater,
                     final Supplier<ClusterState> followerClusterStateSupplier,
                     LongSupplier relativeTimeProvider) {
            this.remoteCluster = remoteCluster;
            this.statsUpdater = statsUpdater;
            this.followerClusterStateSupplier = followerClusterStateSupplier;
            this.relativeTimeProvider = relativeTimeProvider;
        }

        void start() {
            lastAutoFollowTimeInMillis = relativeTimeProvider.getAsLong();
            final ClusterState clusterState = followerClusterStateSupplier.get();
            final AutoFollowMetadata autoFollowMetadata = clusterState.metaData().custom(AutoFollowMetadata.TYPE);
            if (autoFollowMetadata == null) {
                LOGGER.info("AutoFollower for cluster [{}] has stopped, because there is no autofollow metadata", remoteCluster);
                return;
            }

            final List<String> patterns = autoFollowMetadata.getPatterns().entrySet().stream()
                .filter(entry -> entry.getValue().getRemoteCluster().equals(remoteCluster))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
            if (patterns.isEmpty()) {
                LOGGER.info("AutoFollower for cluster [{}] has stopped, because there are no more patterns", remoteCluster);
                return;
            }

            this.autoFollowPatternsCountDown = new CountDown(patterns.size());
            this.autoFollowResults = new AtomicArray<>(patterns.size());

            getRemoteClusterState(remoteCluster, metadataVersion + 1, (remoteClusterStateResponse, remoteError) -> {
                if (remoteClusterStateResponse != null) {
                    assert remoteError == null;
                    if (remoteClusterStateResponse.isWaitForTimedOut()) {
                        start();
                        return;
                    }
                    ClusterState remoteClusterState = remoteClusterStateResponse.getState();
                    metadataVersion = remoteClusterState.metaData().version();
                    autoFollowIndices(autoFollowMetadata, clusterState, remoteClusterState, patterns);
                } else {
                    assert remoteError != null;
                    for (int i = 0; i < patterns.size(); i++) {
                        String autoFollowPatternName = patterns.get(i);
                        finalise(i, new AutoFollowResult(autoFollowPatternName, remoteError));
                    }
                }
            });
        }

        private void autoFollowIndices(final AutoFollowMetadata autoFollowMetadata,
                                       final ClusterState clusterState,
                                       final ClusterState remoteClusterState,
                                       final List<String> patterns) {
            int i = 0;
            for (String autoFollowPatternName : patterns) {
                final int slot = i;
                AutoFollowPattern autoFollowPattern = autoFollowMetadata.getPatterns().get(autoFollowPatternName);
                Map<String, String> headers = autoFollowMetadata.getHeaders().get(autoFollowPatternName);
                List<String> followedIndices = autoFollowMetadata.getFollowedLeaderIndexUUIDs().get(autoFollowPatternName);

                final List<Index> leaderIndicesToFollow =
                    getLeaderIndicesToFollow(autoFollowPattern, remoteClusterState, followedIndices);
                if (leaderIndicesToFollow.isEmpty()) {
                    finalise(slot, new AutoFollowResult(autoFollowPatternName));
                } else {
                    List<Tuple<String, AutoFollowPattern>> patternsForTheSameRemoteCluster = autoFollowMetadata.getPatterns()
                        .entrySet().stream()
                        .filter(item -> autoFollowPatternName.equals(item.getKey()) == false)
                        .filter(item -> remoteCluster.equals(item.getValue().getRemoteCluster()))
                        .map(item -> new Tuple<>(item.getKey(), item.getValue()))
                        .collect(Collectors.toList());

                    Consumer<AutoFollowResult> resultHandler = result -> finalise(slot, result);
                    checkAutoFollowPattern(autoFollowPatternName, remoteCluster, autoFollowPattern, leaderIndicesToFollow, headers,
                        patternsForTheSameRemoteCluster, remoteClusterState.metaData(), clusterState.metaData(), resultHandler);
                }
                i++;
            }
            cleanFollowedRemoteIndices(remoteClusterState, patterns);
        }

        private void checkAutoFollowPattern(String autoFollowPattenName,
                                            String remoteCluster,
                                            AutoFollowPattern autoFollowPattern,
                                            List<Index> leaderIndicesToFollow,
                                            Map<String, String> headers,
                                            List<Tuple<String, AutoFollowPattern>> patternsForTheSameRemoteCluster,
                                            MetaData remoteMetadata,
                                            MetaData localMetadata,
                                            Consumer<AutoFollowResult> resultHandler) {

            final CountDown leaderIndicesCountDown = new CountDown(leaderIndicesToFollow.size());
            final AtomicArray<Tuple<Index, Exception>> results = new AtomicArray<>(leaderIndicesToFollow.size());
            for (int i = 0; i < leaderIndicesToFollow.size(); i++) {
                final Index indexToFollow = leaderIndicesToFollow.get(i);
                final int slot = i;

                List<String> otherMatchingPatterns = patternsForTheSameRemoteCluster.stream()
                    .filter(otherPattern -> otherPattern.v2().match(indexToFollow.getName()))
                    .map(Tuple::v1)
                    .collect(Collectors.toList());
                if (otherMatchingPatterns.size() != 0) {
                    results.set(slot, new Tuple<>(indexToFollow, new ElasticsearchException("index to follow [" + indexToFollow.getName() +
                        "] for pattern [" + autoFollowPattenName + "] matches with other patterns " + otherMatchingPatterns + "")));
                    if (leaderIndicesCountDown.countDown()) {
                        resultHandler.accept(new AutoFollowResult(autoFollowPattenName, results.asList()));
                    }
                } else {
                    final Settings leaderIndexSettings = remoteMetadata.getIndexSafe(indexToFollow).getSettings();
                    if (leaderIndexSettings.getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(),
                        IndexMetaData.SETTING_INDEX_VERSION_CREATED.get(leaderIndexSettings).onOrAfter(Version.V_7_0_0)) == false) {

                        String message = String.format(Locale.ROOT, "index [%s] cannot be followed, because soft deletes are not enabled",
                            indexToFollow.getName());
                        LOGGER.warn(message);
                        updateAutoFollowMetadata(recordLeaderIndexAsFollowFunction(autoFollowPattenName, indexToFollow), error -> {
                            ElasticsearchException failure = new ElasticsearchException(message);
                            if (error != null) {
                                failure.addSuppressed(error);
                            }
                            results.set(slot, new Tuple<>(indexToFollow, failure));
                            if (leaderIndicesCountDown.countDown()) {
                                resultHandler.accept(new AutoFollowResult(autoFollowPattenName, results.asList()));
                            }
                        });
                        continue;
                    } else if (leaderIndexAlreadyFollowed(autoFollowPattern, indexToFollow, localMetadata)) {
                        updateAutoFollowMetadata(recordLeaderIndexAsFollowFunction(autoFollowPattenName, indexToFollow), error -> {
                            results.set(slot, new Tuple<>(indexToFollow, error));
                            if (leaderIndicesCountDown.countDown()) {
                                resultHandler.accept(new AutoFollowResult(autoFollowPattenName, results.asList()));
                            }
                        });
                        continue;
                    }

                    followLeaderIndex(autoFollowPattenName, remoteCluster, indexToFollow, autoFollowPattern, headers, error -> {
                        results.set(slot, new Tuple<>(indexToFollow, error));
                        if (leaderIndicesCountDown.countDown()) {
                            resultHandler.accept(new AutoFollowResult(autoFollowPattenName, results.asList()));
                        }
                    });
                }
            }
        }

        private static boolean leaderIndexAlreadyFollowed(AutoFollowPattern autoFollowPattern,
                                                          Index leaderIndex,
                                                          MetaData localMetadata) {
            String followIndexName = getFollowerIndexName(autoFollowPattern, leaderIndex.getName());
            IndexMetaData indexMetaData = localMetadata.index(followIndexName);
            if (indexMetaData != null) {
                // If an index with the same name exists, but it is not a follow index for this leader index then
                // we should let the auto follower attempt to auto follow it, so it can fail later and
                // it is then visible in the auto follow stats. For example a cluster can just happen to have
                // an index with the same name as the new follower index.
                Map<String, String> customData = indexMetaData.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY);
                if (customData != null) {
                    String recordedLeaderIndexUUID = customData.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY);
                    return leaderIndex.getUUID().equals(recordedLeaderIndexUUID);
                }
            }
            return false;
        }

        private void followLeaderIndex(String autoFollowPattenName,
                                       String remoteCluster,
                                       Index indexToFollow,
                                       AutoFollowPattern pattern,
                                       Map<String,String> headers,
                                       Consumer<Exception> onResult) {
            final String leaderIndexName = indexToFollow.getName();
            final String followIndexName = getFollowerIndexName(pattern, leaderIndexName);

            ResumeFollowAction.Request followRequest = new ResumeFollowAction.Request();
            followRequest.setFollowerIndex(followIndexName);
            followRequest.setMaxReadRequestOperationCount(pattern.getMaxReadRequestOperationCount());
            followRequest.setMaxReadRequestSize(pattern.getMaxReadRequestSize());
            followRequest.setMaxOutstandingReadRequests(pattern.getMaxOutstandingReadRequests());
            followRequest.setMaxWriteRequestOperationCount(pattern.getMaxWriteRequestOperationCount());
            followRequest.setMaxWriteRequestSize(pattern.getMaxWriteRequestSize());
            followRequest.setMaxOutstandingWriteRequests(pattern.getMaxOutstandingWriteRequests());
            followRequest.setMaxWriteBufferCount(pattern.getMaxWriteBufferCount());
            followRequest.setMaxWriteBufferSize(pattern.getMaxWriteBufferSize());
            followRequest.setMaxRetryDelay(pattern.getMaxRetryDelay());
            followRequest.setReadPollTimeout(pattern.getPollTimeout());

            PutFollowAction.Request request = new PutFollowAction.Request();
            request.setRemoteCluster(remoteCluster);
            request.setLeaderIndex(indexToFollow.getName());
            request.setFollowRequest(followRequest);

            // Execute if the create and follow api call succeeds:
            Runnable successHandler = () -> {
                LOGGER.info("Auto followed leader index [{}] as follow index [{}]", leaderIndexName, followIndexName);

                // This function updates the auto follow metadata in the cluster to record that the leader index has been followed:
                // (so that we do not try to follow it in subsequent auto follow runs)
                Function<ClusterState, ClusterState> function = recordLeaderIndexAsFollowFunction(autoFollowPattenName, indexToFollow);
                // The coordinator always runs on the elected master node, so we can update cluster state here:
                updateAutoFollowMetadata(function, onResult);
            };
            createAndFollow(headers, request, successHandler, onResult);
        }

        private void finalise(int slot, AutoFollowResult result) {
            assert autoFollowResults.get(slot) == null;
            autoFollowResults.set(slot, result);
            if (autoFollowPatternsCountDown.countDown()) {
                statsUpdater.accept(autoFollowResults.asList());
                start();
            }
        }

        static List<Index> getLeaderIndicesToFollow(AutoFollowPattern autoFollowPattern,
                                                    ClusterState remoteClusterState,
                                                    List<String> followedIndexUUIDs) {
            List<Index> leaderIndicesToFollow = new ArrayList<>();
            for (IndexMetaData leaderIndexMetaData : remoteClusterState.getMetaData()) {
                if (autoFollowPattern.match(leaderIndexMetaData.getIndex().getName())) {
                    IndexRoutingTable indexRoutingTable = remoteClusterState.routingTable().index(leaderIndexMetaData.getIndex());
                    if (indexRoutingTable != null &&
                        // Leader indices can be in the cluster state, but not all primary shards may be ready yet.
                        // This checks ensures all primary shards have started, so that index following does not fail.
                        // If not all primary shards are ready, then the next time the auto follow coordinator runs
                        // this index will be auto followed.
                        indexRoutingTable.allPrimaryShardsActive() &&
                        followedIndexUUIDs.contains(leaderIndexMetaData.getIndex().getUUID()) == false) {

                        leaderIndicesToFollow.add(leaderIndexMetaData.getIndex());
                    }
                }
            }
            return leaderIndicesToFollow;
        }

        static String getFollowerIndexName(AutoFollowPattern autoFollowPattern, String leaderIndexName) {
            if (autoFollowPattern.getFollowIndexPattern() != null) {
                return autoFollowPattern.getFollowIndexPattern().replace("{{leader_index}}", leaderIndexName);
            } else {
                return leaderIndexName;
            }
        }

        static Function<ClusterState, ClusterState> recordLeaderIndexAsFollowFunction(String name,
                                                                                      Index indexToFollow) {
            return currentState -> {
                AutoFollowMetadata currentAutoFollowMetadata = currentState.metaData().custom(AutoFollowMetadata.TYPE);
                Map<String, List<String>> newFollowedIndexUUIDS = new HashMap<>(currentAutoFollowMetadata.getFollowedLeaderIndexUUIDs());
                if (newFollowedIndexUUIDS.containsKey(name) == false) {
                    // A delete auto follow pattern request can have removed the auto follow pattern while we want to update
                    // the auto follow metadata with the fact that an index was successfully auto followed. If this
                    // happens, we can just skip this step.
                    return currentState;
                }

                newFollowedIndexUUIDS.compute(name, (key, existingUUIDs) -> {
                    assert existingUUIDs != null;
                    List<String> newUUIDs = new ArrayList<>(existingUUIDs);
                    newUUIDs.add(indexToFollow.getUUID());
                    return Collections.unmodifiableList(newUUIDs);
                });
                final AutoFollowMetadata newAutoFollowMetadata = new AutoFollowMetadata(currentAutoFollowMetadata.getPatterns(),
                    newFollowedIndexUUIDS, currentAutoFollowMetadata.getHeaders());
                return ClusterState.builder(currentState)
                    .metaData(MetaData.builder(currentState.getMetaData())
                        .putCustom(AutoFollowMetadata.TYPE, newAutoFollowMetadata).build())
                    .build();
            };
        }

        void cleanFollowedRemoteIndices(final ClusterState remoteClusterState, final List<String> patterns) {
            updateAutoFollowMetadata(cleanFollowedRemoteIndices(remoteClusterState.metaData(), patterns), e -> {
                if (e != null) {
                    LOGGER.warn("Error occured while cleaning followed leader indices", e);
                }
            });
        }

        static Function<ClusterState, ClusterState> cleanFollowedRemoteIndices(
            final MetaData remoteMetadata, final List<String> autoFollowPatternNames) {
            return currentState -> {
                AutoFollowMetadata currentAutoFollowMetadata = currentState.metaData().custom(AutoFollowMetadata.TYPE);
                Map<String, List<String>> autoFollowPatternNameToFollowedIndexUUIDs =
                    new HashMap<>(currentAutoFollowMetadata.getFollowedLeaderIndexUUIDs());
                Set<String> remoteIndexUUIDS = new HashSet<>();
                remoteMetadata.getIndices().values()
                    .forEach((ObjectPredicate<IndexMetaData>) value -> remoteIndexUUIDS.add(value.getIndexUUID()));

                boolean requiresCSUpdate = false;
                for (String autoFollowPatternName : autoFollowPatternNames) {
                    if (autoFollowPatternNameToFollowedIndexUUIDs.containsKey(autoFollowPatternName) == false) {
                        // A delete auto follow pattern request can have removed the auto follow pattern while we want to update
                        // the auto follow metadata with the fact that an index was successfully auto followed. If this
                        // happens, we can just skip this step.
                        continue;
                    }

                    List<String> followedIndexUUIDs =
                        new ArrayList<>(autoFollowPatternNameToFollowedIndexUUIDs.get(autoFollowPatternName));
                    // Remove leader indices that no longer exist in the remote cluster:
                    boolean entriesRemoved = followedIndexUUIDs.removeIf(
                        followedLeaderIndexUUID -> remoteIndexUUIDS.contains(followedLeaderIndexUUID) == false);
                    if (entriesRemoved) {
                        requiresCSUpdate = true;
                    }
                    autoFollowPatternNameToFollowedIndexUUIDs.put(autoFollowPatternName, followedIndexUUIDs);
                }

                if (requiresCSUpdate) {
                    final AutoFollowMetadata newAutoFollowMetadata = new AutoFollowMetadata(currentAutoFollowMetadata.getPatterns(),
                        autoFollowPatternNameToFollowedIndexUUIDs, currentAutoFollowMetadata.getHeaders());
                    return ClusterState.builder(currentState)
                        .metaData(MetaData.builder(currentState.getMetaData())
                            .putCustom(AutoFollowMetadata.TYPE, newAutoFollowMetadata).build())
                        .build();
                } else {
                    return currentState;
                }
            };
        }

        /**
         * Fetch a remote cluster state from with the specified cluster alias
         * @param remoteCluster      the name of the leader cluster
         * @param metadataVersion   the last seen metadata version
         * @param handler            the callback to invoke
         */
        abstract void getRemoteClusterState(
            String remoteCluster,
            long metadataVersion,
            BiConsumer<ClusterStateResponse, Exception> handler
        );

        abstract void createAndFollow(
            Map<String, String> headers,
            PutFollowAction.Request followRequest,
            Runnable successHandler,
            Consumer<Exception> failureHandler
        );

        abstract void updateAutoFollowMetadata(
            Function<ClusterState, ClusterState> updateFunction,
            Consumer<Exception> handler
        );

    }

    static class AutoFollowResult {

        final String autoFollowPatternName;
        final Exception clusterStateFetchException;
        final Map<Index, Exception> autoFollowExecutionResults;

        AutoFollowResult(String autoFollowPatternName, List<Tuple<Index, Exception>> results) {
            this.autoFollowPatternName = autoFollowPatternName;

            Map<Index, Exception> autoFollowExecutionResults = new HashMap<>();
            for (Tuple<Index, Exception> result : results) {
                autoFollowExecutionResults.put(result.v1(), result.v2());
            }

            this.clusterStateFetchException = null;
            this.autoFollowExecutionResults = Collections.unmodifiableMap(autoFollowExecutionResults);
        }

        AutoFollowResult(String autoFollowPatternName, Exception e) {
            this.autoFollowPatternName = autoFollowPatternName;
            this.clusterStateFetchException = e;
            this.autoFollowExecutionResults = Collections.emptyMap();
        }

        AutoFollowResult(String autoFollowPatternName) {
            this(autoFollowPatternName, (Exception) null);
        }
    }
}
