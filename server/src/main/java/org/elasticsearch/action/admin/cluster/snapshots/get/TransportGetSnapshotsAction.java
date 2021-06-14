/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.repositories.get.TransportGetRepositoriesAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.repositories.GetSnapshotInfoContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Transport Action for get snapshots operation
 */
public class TransportGetSnapshotsAction extends TransportMasterNodeAction<GetSnapshotsRequest, GetSnapshotsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetSnapshotsAction.class);

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportGetSnapshotsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        RepositoriesService repositoriesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetSnapshotsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetSnapshotsRequest::new,
            indexNameExpressionResolver,
            GetSnapshotsResponse::new,
            ThreadPool.Names.SAME
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshotsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(
        Task task,
        final GetSnapshotsRequest request,
        final ClusterState state,
        final ActionListener<GetSnapshotsResponse> listener
    ) {
        assert task instanceof CancellableTask : task + " not cancellable";

        getMultipleReposSnapshotInfo(
            state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY),
            TransportGetRepositoriesAction.getRepositories(state, request.repositories()),
            request.snapshots(),
            request.ignoreUnavailable(),
            request.verbose(),
            (CancellableTask) task,
            request.size(),
            request.sort(),
            request.after(),
            request.order(),
            listener
        );
    }

    private void getMultipleReposSnapshotInfo(
        SnapshotsInProgress snapshotsInProgress,
        List<RepositoryMetadata> repos,
        String[] snapshots,
        boolean ignoreUnavailable,
        boolean verbose,
        CancellableTask cancellableTask,
        int size,
        GetSnapshotsRequest.SortBy sortBy,
        @Nullable GetSnapshotsRequest.After after,
        SortOrder order,
        ActionListener<GetSnapshotsResponse> listener
    ) {
        // short-circuit if there are no repos, because we can not create GroupedActionListener of size 0
        if (repos.isEmpty()) {
            listener.onResponse(new GetSnapshotsResponse(Collections.emptyList()));
            return;
        }
        final GroupedActionListener<GetSnapshotsResponse.Response> groupedActionListener = new GroupedActionListener<>(
            listener.map(responses -> {
                assert repos.size() == responses.size();
                return new GetSnapshotsResponse(responses);
            }),
            repos.size()
        );

        for (final RepositoryMetadata repo : repos) {
            final String repoName = repo.name();
            getSingleRepoSnapshotInfo(
                snapshotsInProgress,
                repoName,
                snapshots,
                ignoreUnavailable,
                verbose,
                cancellableTask,
                sortBy,
                size,
                after,
                order,
                groupedActionListener.delegateResponse((groupedListener, e) -> {
                    if (e instanceof ElasticsearchException) {
                        groupedListener.onResponse(GetSnapshotsResponse.Response.error(repoName, (ElasticsearchException) e));
                    } else {
                        groupedListener.onFailure(e);
                    }
                }).map(snInfos -> GetSnapshotsResponse.Response.snapshots(repoName, snInfos))
            );
        }
    }

    private void getSingleRepoSnapshotInfo(
        SnapshotsInProgress snapshotsInProgress,
        String repo,
        String[] snapshots,
        boolean ignoreUnavailable,
        boolean verbose,
        CancellableTask task,
        GetSnapshotsRequest.SortBy sortBy,
        int size,
        @Nullable final GetSnapshotsRequest.After after,
        SortOrder order,
        ActionListener<List<SnapshotInfo>> listener
    ) {
        final Map<String, SnapshotId> allSnapshotIds = new HashMap<>();
        final List<SnapshotInfo> currentSnapshots = new ArrayList<>();
        for (SnapshotInfo snapshotInfo : sortedCurrentSnapshots(snapshotsInProgress, repo, size, after, sortBy, order)) {
            SnapshotId snapshotId = snapshotInfo.snapshotId();
            allSnapshotIds.put(snapshotId.getName(), snapshotId);
            currentSnapshots.add(snapshotInfo);
        }

        final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
        if (isCurrentSnapshotsOnly(snapshots)) {
            repositoryDataListener.onResponse(null);
        } else {
            repositoriesService.getRepositoryData(repo, repositoryDataListener);
        }

        repositoryDataListener.whenComplete(
            repositoryData -> loadSnapshotInfos(
                snapshotsInProgress,
                repo,
                snapshots,
                ignoreUnavailable,
                verbose,
                allSnapshotIds,
                currentSnapshots,
                repositoryData,
                task,
                size,
                after,
                sortBy,
                order,
                listener
            ),
            listener::onFailure
        );
    }

    /**
     * Returns a list of currently running snapshots from repository sorted by snapshot creation date
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repositoryName repository name
     * @return list of snapshots
     */
    private static List<SnapshotInfo> sortedCurrentSnapshots(
        SnapshotsInProgress snapshotsInProgress,
        String repositoryName,
        int size,
        @Nullable final GetSnapshotsRequest.After after,
        GetSnapshotsRequest.SortBy sortBy,
        SortOrder order
    ) {
        List<SnapshotInfo> snapshotList = new ArrayList<>();
        List<SnapshotsInProgress.Entry> entries = SnapshotsService.currentSnapshots(
            snapshotsInProgress,
            repositoryName,
            Collections.emptyList()
        );
        for (SnapshotsInProgress.Entry entry : entries) {
            snapshotList.add(new SnapshotInfo(entry));
        }
        return sortSnapshots(snapshotList, sortBy, after, size, order);
    }

    private void loadSnapshotInfos(
        SnapshotsInProgress snapshotsInProgress,
        String repo,
        String[] snapshots,
        boolean ignoreUnavailable,
        boolean verbose,
        Map<String, SnapshotId> allSnapshotIds,
        List<SnapshotInfo> currentSnapshots,
        @Nullable RepositoryData repositoryData,
        CancellableTask task,
        int size,
        @Nullable final GetSnapshotsRequest.After after,
        GetSnapshotsRequest.SortBy sortBy,
        SortOrder order,
        ActionListener<List<SnapshotInfo>> listener
    ) {
        if (task.isCancelled()) {
            listener.onFailure(new TaskCancelledException("task cancelled"));
            return;
        }

        if (repositoryData != null) {
            for (SnapshotId snapshotId : repositoryData.getSnapshotIds()) {
                allSnapshotIds.put(snapshotId.getName(), snapshotId);
            }
        }

        final Set<SnapshotId> toResolve = new HashSet<>();
        if (isAllSnapshots(snapshots)) {
            toResolve.addAll(allSnapshotIds.values());
        } else {
            for (String snapshotOrPattern : snapshots) {
                if (GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshotOrPattern)) {
                    toResolve.addAll(currentSnapshots.stream().map(SnapshotInfo::snapshotId).collect(Collectors.toList()));
                } else if (Regex.isSimpleMatchPattern(snapshotOrPattern) == false) {
                    if (allSnapshotIds.containsKey(snapshotOrPattern)) {
                        toResolve.add(allSnapshotIds.get(snapshotOrPattern));
                    } else if (ignoreUnavailable == false) {
                        throw new SnapshotMissingException(repo, snapshotOrPattern);
                    }
                } else {
                    for (Map.Entry<String, SnapshotId> entry : allSnapshotIds.entrySet()) {
                        if (Regex.simpleMatch(snapshotOrPattern, entry.getKey())) {
                            toResolve.add(entry.getValue());
                        }
                    }
                }
            }

            if (toResolve.isEmpty() && ignoreUnavailable == false && isCurrentSnapshotsOnly(snapshots) == false) {
                throw new SnapshotMissingException(repo, snapshots[0]);
            }
        }

        if (verbose) {
            snapshots(snapshotsInProgress, repo, toResolve, ignoreUnavailable, task, sortBy, size, after, order, listener);
        } else {
            final List<SnapshotInfo> snapshotInfos;
            if (repositoryData != null) {
                // want non-current snapshots as well, which are found in the repository data
                snapshotInfos = buildSimpleSnapshotInfos(toResolve, repositoryData, currentSnapshots, size, after, sortBy, order);
            } else {
                // only want current snapshots
                snapshotInfos = sortSnapshots(
                    currentSnapshots.stream().map(SnapshotInfo::basic).collect(Collectors.toList()),
                    sortBy,
                    after,
                    size,
                    order
                );
            }
            listener.onResponse(snapshotInfos);
        }
    }

    /**
     * Returns a list of snapshots from repository sorted by snapshot creation date
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repositoryName      repository name
     * @param snapshotIds         snapshots for which to fetch snapshot information
     * @param ignoreUnavailable   if true, snapshots that could not be read will only be logged with a warning,
     *                            if false, they will throw an error
     */
    private void snapshots(
        SnapshotsInProgress snapshotsInProgress,
        String repositoryName,
        Collection<SnapshotId> snapshotIds,
        boolean ignoreUnavailable,
        CancellableTask task,
        GetSnapshotsRequest.SortBy sortBy,
        int size,
        @Nullable GetSnapshotsRequest.After after,
        SortOrder order,
        ActionListener<List<SnapshotInfo>> listener
    ) {
        if (task.isCancelled()) {
            listener.onFailure(new TaskCancelledException("task cancelled"));
            return;
        }
        final Set<SnapshotInfo> snapshotSet = new HashSet<>();
        final Set<SnapshotId> snapshotIdsToIterate = new HashSet<>(snapshotIds);
        // first, look at the snapshots in progress
        final List<SnapshotsInProgress.Entry> entries = SnapshotsService.currentSnapshots(
            snapshotsInProgress,
            repositoryName,
            snapshotIdsToIterate.stream().map(SnapshotId::getName).collect(Collectors.toList())
        );
        for (SnapshotsInProgress.Entry entry : entries) {
            if (snapshotIdsToIterate.remove(entry.snapshot().getSnapshotId())) {
                snapshotSet.add(new SnapshotInfo(entry));
            }
        }
        // then, look in the repository if there's any matching snapshots left
        final List<SnapshotInfo> snapshotInfos;
        if (snapshotIdsToIterate.isEmpty()) {
            snapshotInfos = Collections.emptyList();
        } else {
            snapshotInfos = Collections.synchronizedList(new ArrayList<>());
        }
        final ActionListener<Void> allDoneListener = listener.delegateFailure((l, v) -> {
            final ArrayList<SnapshotInfo> snapshotList = new ArrayList<>(snapshotInfos);
            snapshotList.addAll(snapshotSet);
            listener.onResponse(sortSnapshots(snapshotList, sortBy, after, size, order));
        });
        if (snapshotIdsToIterate.isEmpty()) {
            allDoneListener.onResponse(null);
            return;
        }
        final Repository repository;
        try {
            repository = repositoriesService.repository(repositoryName);
        } catch (RepositoryMissingException e) {
            listener.onFailure(e);
            return;
        }
        repository.getSnapshotInfo(
            new GetSnapshotInfoContext(
                snapshotIdsToIterate,
                ignoreUnavailable == false,
                task::isCancelled,
                (context, snapshotInfo) -> snapshotInfos.add(snapshotInfo),
                ignoreUnavailable ? ActionListener.runAfter(new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        logger.trace("done fetching snapshot infos [{}]", snapshotIdsToIterate);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        assert false : new AssertionError("listener should always complete successfully for ignoreUnavailable=true", e);
                        logger.warn("failed to fetch snapshot info for some snapshots", e);
                    }
                }, () -> allDoneListener.onResponse(null)) : allDoneListener
            )
        );
    }

    private boolean isAllSnapshots(String[] snapshots) {
        return (snapshots.length == 0) || (snapshots.length == 1 && GetSnapshotsRequest.ALL_SNAPSHOTS.equalsIgnoreCase(snapshots[0]));
    }

    private boolean isCurrentSnapshotsOnly(String[] snapshots) {
        return (snapshots.length == 1 && GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshots[0]));
    }

    private static List<SnapshotInfo> buildSimpleSnapshotInfos(
        final Set<SnapshotId> toResolve,
        final RepositoryData repositoryData,
        final List<SnapshotInfo> currentSnapshots,
        final int size,
        @Nullable final GetSnapshotsRequest.After after,
        final GetSnapshotsRequest.SortBy sortBy,
        final SortOrder order
    ) {
        List<SnapshotInfo> snapshotInfos = new ArrayList<>();
        for (SnapshotInfo snapshotInfo : currentSnapshots) {
            if (toResolve.remove(snapshotInfo.snapshotId())) {
                snapshotInfos.add(snapshotInfo.basic());
            }
        }
        Map<SnapshotId, List<String>> snapshotsToIndices = new HashMap<>();
        for (IndexId indexId : repositoryData.getIndices().values()) {
            for (SnapshotId snapshotId : repositoryData.getSnapshots(indexId)) {
                if (toResolve.contains(snapshotId)) {
                    snapshotsToIndices.computeIfAbsent(snapshotId, (k) -> new ArrayList<>()).add(indexId.getName());
                }
            }
        }
        for (SnapshotId snapshotId : toResolve) {
            final List<String> indices = snapshotsToIndices.getOrDefault(snapshotId, Collections.emptyList());
            CollectionUtil.timSort(indices);
            snapshotInfos.add(
                new SnapshotInfo(
                    snapshotId,
                    indices,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    repositoryData.getSnapshotState(snapshotId)
                )
            );
        }
        return sortSnapshots(snapshotInfos, sortBy, after, size, order);
    }

    private static final Comparator<SnapshotInfo> BY_START_TIME = Comparator.comparing(SnapshotInfo::startTime)
        .thenComparing(SnapshotInfo::snapshotId);

    private static final Comparator<SnapshotInfo> BY_DURATION = Comparator.<SnapshotInfo, Long>comparing(
        sni -> sni.endTime() - sni.startTime()
    ).thenComparing(SnapshotInfo::snapshotId);

    private static final Comparator<SnapshotInfo> BY_INDICES_COUNT = Comparator.<SnapshotInfo, Integer>comparing(
        sni -> sni.indices().size()
    ).thenComparing(SnapshotInfo::snapshotId);

    private static final Comparator<SnapshotInfo> BY_NAME = Comparator.comparing(sni -> sni.snapshotId().getName());

    private static List<SnapshotInfo> sortSnapshots(
        List<SnapshotInfo> snapshotInfos,
        GetSnapshotsRequest.SortBy sortBy,
        @Nullable GetSnapshotsRequest.After after,
        int size,
        SortOrder order
    ) {
        final Comparator<SnapshotInfo> comparator;
        switch (sortBy) {
            case START_TIME:
                comparator = BY_START_TIME;
                break;
            case NAME:
                comparator = BY_NAME;
                break;
            case DURATION:
                comparator = BY_DURATION;
                break;
            case INDICES:
                comparator = BY_INDICES_COUNT;
                break;
            default:
                throw new AssertionError("unexpected sort column [" + sortBy + "]");
        }
        CollectionUtil.timSort(snapshotInfos, order == SortOrder.DESC ? comparator.reversed() : comparator);
        // TODO: support asc order + make use of it below as well
        int startIndex = 0;
        if (after != null) {
            switch (sortBy) {
                case START_TIME:
                    final long start = Long.parseLong(after.value());
                    if (order == SortOrder.ASC) {
                        for (int i = 0; i < snapshotInfos.size(); i++) {
                            final SnapshotInfo info = snapshotInfos.get(i);
                            if (startIsAfter(after, start, info)) {
                                startIndex = i;
                                break;
                            }
                        }
                    } else {
                        for (int i = 0; i < snapshotInfos.size(); i++) {
                            final SnapshotInfo info = snapshotInfos.get(i);
                            if (startIsBefore(after, start, info)) {
                                startIndex = i;
                                break;
                            }
                        }
                    }
                    break;
                case NAME:
                    if (order == SortOrder.ASC) {
                        for (int i = 0; i < snapshotInfos.size(); i++) {
                            if (nameIsAfter(after.snapshotName(), snapshotInfos.get(i))) {
                                startIndex = i;
                                break;
                            }
                        }
                    } else {
                        for (int i = 0; i < snapshotInfos.size(); i++) {
                            if (nameIsBefore(after.snapshotName(), snapshotInfos.get(i))) {
                                startIndex = i;
                                break;
                            }
                        }
                    }
                    break;
                case DURATION:
                    final long duration = Long.parseLong(after.value());
                    for (int i = 0; i < snapshotInfos.size(); i++) {
                        final SnapshotInfo info = snapshotInfos.get(i);
                        final long snapshotDuration = info.endTime() - info.startTime();
                        if (duration < snapshotDuration || (duration == snapshotDuration && nameIsAfter(after.snapshotName(), info))) {
                            startIndex = i;
                            break;
                        }
                    }
                    break;
                case INDICES:
                    final int indices = Integer.parseInt(after.value());
                    for (int i = 0; i < snapshotInfos.size(); i++) {
                        final SnapshotInfo info = snapshotInfos.get(i);
                        final int indexCount = info.indices().size();
                        if (indices < indexCount || (indices == indexCount && nameIsAfter(after.snapshotName(), info))) {
                            startIndex = i;
                            break;
                        }
                    }
                    break;
            }
        }
        final List<SnapshotInfo> afterStart;
        if (startIndex == 0) {
            afterStart = snapshotInfos;
        } else {
            afterStart = snapshotInfos.subList(startIndex, snapshotInfos.size());
        }
        return List.copyOf(size > 0 && size < afterStart.size() ? afterStart.subList(0, size) : afterStart);
    }

    private static boolean startIsAfter(@Nullable GetSnapshotsRequest.After after, long start, SnapshotInfo info) {
        return start < info.startTime() || (start == info.startTime() && nameIsAfter(after.snapshotName(), info));
    }

    private static boolean startIsBefore(@Nullable GetSnapshotsRequest.After after, long start, SnapshotInfo info) {
        return start > info.startTime() || (start == info.startTime() && nameIsBefore(after.snapshotName(), info));
    }

    private static boolean nameIsAfter(String afterName, SnapshotInfo info) {
        return afterName.compareTo(info.snapshotId().getName()) < 0;
    }

    private static boolean nameIsBefore(String afterName, SnapshotInfo info) {
        return afterName.compareTo(info.snapshotId().getName()) > 0;
    }
}
