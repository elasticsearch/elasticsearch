/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.TransportSearchAction.SearchTimeProvider;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

/**
 * This is an abstract base class that encapsulates the logic to fan out to all shards in provided {@link GroupShardsIterator}
 * and collect the results. If a shard request returns a failure this class handles the advance to the next replica of the shard until
 * the shards replica iterator is exhausted. Each shard is referenced by position in the {@link GroupShardsIterator} which is later
 * referred to as the {@code shardIndex}.
 * The fan out and collect algorithm is traditionally used as the initial phase which can either be a query execution or collection of
 * distributed frequencies
 */
abstract class AbstractSearchAsyncAction<Result extends SearchPhaseResult> extends AsyncSearchContext<Result> {
    static final float DEFAULT_INDEX_BOOST = 1.0f;
    private final Logger logger;
    private final SearchTransportService searchTransportService;
    private final Executor executor;

    /**
     * Used by subclasses to resolve node ids to DiscoveryNodes.
     **/
    private final BiFunction<String, String, Transport.Connection> nodeIdToConnection;
    private final SearchTask task;
    private final long clusterStateVersion;
    private final TransportVersion minTransportVersion;
    private final Map<String, AliasFilter> aliasFilter;
    private final Map<String, Float> concreteIndexBoosts;
    private final SetOnce<AtomicArray<ShardSearchFailure>> shardFailures = new SetOnce<>();
    private final Object shardFailuresMutex = new Object();
    private final AtomicBoolean hasShardResponse = new AtomicBoolean(false);
    private final AtomicInteger successfulOps = new AtomicInteger();
    private final AtomicInteger skippedOps = new AtomicInteger();
    private final SearchTimeProvider timeProvider;
    private final SearchResponse.Clusters clusters;

    protected final GroupShardsIterator<SearchShardIterator> toSkipShardsIts;
    protected final GroupShardsIterator<SearchShardIterator> shardsIts;
    private final SearchShardIterator[] shardIterators;
    private final int expectedTotalOps;
    private final AtomicInteger totalOps = new AtomicInteger();
    private final int maxConcurrentRequestsPerNode;
    private final Map<String, PendingExecutions> pendingExecutionsPerNode = new ConcurrentHashMap<>();
    private final boolean throttleConcurrentRequests;
    private final AtomicBoolean requestCancelled = new AtomicBoolean();

    // protected for tests
    protected final String name;

    AbstractSearchAsyncAction(
        String name,
        Logger logger,
        NamedWriteableRegistry namedWriteableRegistry,
        SearchTransportService searchTransportService,
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Executor executor,
        SearchRequest request,
        ActionListener<SearchResponse> listener,
        GroupShardsIterator<SearchShardIterator> shardsIts,
        SearchTimeProvider timeProvider,
        ClusterState clusterState,
        SearchTask task,
        SearchPhaseResults<Result> resultConsumer,
        int maxConcurrentRequestsPerNode,
        SearchResponse.Clusters clusters
    ) {
        super(request, resultConsumer, namedWriteableRegistry, listener);
        this.name = name;
        final List<SearchShardIterator> toSkipIterators = new ArrayList<>();
        final List<SearchShardIterator> iterators = new ArrayList<>();
        for (final SearchShardIterator iterator : shardsIts) {
            if (iterator.skip()) {
                toSkipIterators.add(iterator);
            } else {
                iterators.add(iterator);
            }
        }
        this.toSkipShardsIts = new GroupShardsIterator<>(toSkipIterators);
        this.shardsIts = new GroupShardsIterator<>(iterators);

        this.shardIterators = iterators.toArray(new SearchShardIterator[0]);
        // we later compute the shard index based on the natural order of the shards
        // that participate in the search request. This means that this number is
        // consistent between two requests that target the same shards.
        Arrays.sort(shardIterators);

        // we need to add 1 for non active partition, since we count it in the total. This means for each shard in the iterator we sum up
        // it's number of active shards but use 1 as the default if no replica of a shard is active at this point.
        // on a per shards level we use shardIt.remaining() to increment the totalOps pointer but add 1 for the current shard result
        // we process hence we add one for the non active partition here.
        this.expectedTotalOps = shardsIts.totalSizeWith1ForEmpty();
        this.maxConcurrentRequestsPerNode = maxConcurrentRequestsPerNode;
        // in the case were we have less shards than maxConcurrentRequestsPerNode we don't need to throttle
        this.throttleConcurrentRequests = maxConcurrentRequestsPerNode < shardsIts.size();
        this.timeProvider = timeProvider;
        this.logger = logger;
        this.searchTransportService = searchTransportService;
        this.executor = executor;
        this.task = task;
        this.nodeIdToConnection = nodeIdToConnection;
        this.concreteIndexBoosts = concreteIndexBoosts;
        this.clusterStateVersion = clusterState.version();
        this.minTransportVersion = clusterState.getMinTransportVersion();
        this.aliasFilter = aliasFilter;
        this.clusters = clusters;
    }

    protected void notifyListShards(
        SearchProgressListener progressListener,
        SearchResponse.Clusters clusters,
        SearchSourceBuilder sourceBuilder
    ) {
        progressListener.notifyListShards(
            SearchProgressListener.buildSearchShards(this.shardsIts),
            SearchProgressListener.buildSearchShards(toSkipShardsIts),
            clusters,
            sourceBuilder == null || sourceBuilder.size() > 0,
            timeProvider
        );
    }

    protected String missingShardsErrorMessage(StringBuilder missingShards) {
        return makeMissingShardsError(missingShards);
    }

    protected static String makeMissingShardsError(StringBuilder missingShards) {
        return "Search rejected due to missing shards ["
            + missingShards
            + "]. Consider using `allow_partial_search_results` setting to bypass this error.";
    }

    protected void doCheckNoMissingShards(String phaseName, SearchRequest request, GroupShardsIterator<SearchShardIterator> shardsIts) {
        doCheckNoMissingShards(phaseName, request, shardsIts, this::missingShardsErrorMessage);
    }

    protected static void doCheckNoMissingShards(
        String phaseName,
        SearchRequest request,
        GroupShardsIterator<SearchShardIterator> shardsIts,
        Function<StringBuilder, String> makeErrorMessage
    ) {
        assert request.allowPartialSearchResults() != null : "SearchRequest missing setting for allowPartialSearchResults";
        if (request.allowPartialSearchResults() == false) {
            final StringBuilder missingShards = new StringBuilder();
            // Fail-fast verification of all shards being available
            for (int index = 0; index < shardsIts.size(); index++) {
                final SearchShardIterator shardRoutings = shardsIts.get(index);
                if (shardRoutings.size() == 0) {
                    if (missingShards.isEmpty() == false) {
                        missingShards.append(", ");
                    }
                    missingShards.append(shardRoutings.shardId());
                }
            }
            if (missingShards.isEmpty() == false) {
                // Status red - shard is missing all copies and would produce partial results for an index search
                final String msg = makeErrorMessage.apply(missingShards);
                throw new SearchPhaseExecutionException(phaseName, msg, null, ShardSearchFailure.EMPTY_ARRAY);
            }
        }
    }

    /**
     * Builds how long it took to execute the search.
     */
    long buildTookInMillis() {
        return timeProvider.buildTookInMillis();
    }

    /**
     * This is the main entry point for a search. This method starts the search execution of the initial phase.
     */
    public final void start() {
        if (results.getNumShards() == 0) {
            // no search shards to search on, bail with empty response
            // (it happens with search across _all with no indices around and consistent with broadcast operations)
            int trackTotalHitsUpTo = request.source() == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
                : request.source().trackTotalHitsUpTo() == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
                : request.source().trackTotalHitsUpTo();
            // total hits is null in the response if the tracking of total hits is disabled
            boolean withTotalHits = trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_DISABLED;
            sendSearchResponse(
                withTotalHits ? SearchResponseSections.EMPTY_WITH_TOTAL_HITS : SearchResponseSections.EMPTY_WITHOUT_TOTAL_HITS,
                new AtomicArray<>(0)
            );
            return;
        }
        try {
            run();
        } catch (RuntimeException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(() -> format("Failed to execute [%s] while moving to [%s] phase", request, name), e);
            }
            onPhaseFailure(name, "", e);
        }
    }

    private void run() {
        for (final SearchShardIterator iterator : toSkipShardsIts) {
            assert iterator.skip();
            skipShard(iterator);
        }
        final Map<SearchShardIterator, Integer> shardIndexMap = Maps.newHashMapWithExpectedSize(shardIterators.length);
        for (int i = 0; i < shardIterators.length; i++) {
            shardIndexMap.put(shardIterators[i], i);
        }
        if (shardsIts.size() > 0) {
            doCheckNoMissingShards(name, request, shardsIts);
            for (int i = 0; i < shardsIts.size(); i++) {
                final SearchShardIterator shardRoutings = shardsIts.get(i);
                assert shardRoutings.skip() == false;
                assert shardIndexMap.containsKey(shardRoutings);
                int shardIndex = shardIndexMap.get(shardRoutings);
                final SearchShardTarget routing = shardRoutings.nextOrNull();
                if (routing == null) {
                    failOnUnavailable(shardIndex, shardRoutings);
                } else {
                    performPhaseOnShard(shardIndex, shardRoutings, routing);
                }
            }
        }
    }

    void skipShard(SearchShardIterator iterator) {
        successfulOps.incrementAndGet();
        skippedOps.incrementAndGet();
        assert iterator.skip();
        successfulShardExecution(iterator);
    }

    private void performPhaseOnShard(final int shardIndex, final SearchShardIterator shardIt, final SearchShardTarget shard) {
        if (throttleConcurrentRequests) {
            var pendingExecutions = pendingExecutionsPerNode.computeIfAbsent(
                shard.getNodeId(),
                n -> new PendingExecutions(maxConcurrentRequestsPerNode)
            );
            pendingExecutions.submit(l -> doPerformPhaseOnShard(shardIndex, shardIt, shard, l));
        } else {
            doPerformPhaseOnShard(shardIndex, shardIt, shard, () -> {});
        }
    }

    private void doPerformPhaseOnShard(int shardIndex, SearchShardIterator shardIt, SearchShardTarget shard, Releasable releasable) {
        var shardListener = new SearchActionListener<Result>(shard, shardIndex) {
            @Override
            public void innerOnResponse(Result result) {
                try {
                    releasable.close();
                    onShardResult(result, shardIt);
                } catch (Exception exc) {
                    onShardFailure(shardIndex, shard, shardIt, exc);
                }
            }

            @Override
            public void onFailure(Exception e) {
                releasable.close();
                onShardFailure(shardIndex, shard, shardIt, e);
            }
        };
        final Transport.Connection connection;
        try {
            connection = getConnection(shard.getClusterAlias(), shard.getNodeId());
        } catch (Exception e) {
            shardListener.onFailure(e);
            return;
        }
        executePhaseOnShard(shardIt, connection, shardListener);
    }

    private void failOnUnavailable(int shardIndex, SearchShardIterator shardIt) {
        SearchShardTarget unassignedShard = new SearchShardTarget(null, shardIt.shardId(), shardIt.getClusterAlias());
        onShardFailure(shardIndex, unassignedShard, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
    }

    /**
     * Sends the request to the actual shard.
     * @param shardIt the shards iterator
     * @param connection to node that the shard is located on
     * @param listener the listener to notify on response
     */
    protected abstract void executePhaseOnShard(
        SearchShardIterator shardIt,
        Transport.Connection connection,
        SearchActionListener<Result> listener
    );

    /**
     * Processes the phase transition from on phase to another. This method handles all errors that happen during the initial run execution
     * of the next phase. If there are no successful operations in the context when this method is executed the search is aborted and
     * a response is returned to the user indicating that all shards have failed.
     */
    public void executeNextPhase(String currentPhase, Supplier<SearchPhase> nextPhaseSupplier) {
        /* This is the main search phase transition where we move to the next phase. If all shards
         * failed or if there was a failure and partial results are not allowed, then we immediately
         * fail. Otherwise we continue to the next phase.
         */
        ShardOperationFailedException[] shardSearchFailures = buildShardFailures(shardFailures);
        final int numShards = results.getNumShards();
        if (shardSearchFailures.length == numShards) {
            shardSearchFailures = ExceptionsHelper.groupBy(shardSearchFailures);
            Throwable cause = shardSearchFailures.length == 0
                ? null
                : ElasticsearchException.guessRootCauses(shardSearchFailures[0].getCause())[0];
            logger.debug(() -> "All shards failed for phase: [" + currentPhase + "]", cause);
            onPhaseFailure(currentPhase, "all shards failed", cause);
        } else {
            Boolean allowPartialResults = request.allowPartialSearchResults();
            assert allowPartialResults != null : "SearchRequest missing setting for allowPartialSearchResults";
            if (allowPartialResults == false && successfulOps.get() != numShards) {
                // check if there are actual failures in the atomic array since
                // successful retries can reset the failures to null
                if (shardSearchFailures.length > 0) {
                    if (logger.isDebugEnabled()) {
                        int numShardFailures = shardSearchFailures.length;
                        shardSearchFailures = ExceptionsHelper.groupBy(shardSearchFailures);
                        Throwable cause = ElasticsearchException.guessRootCauses(shardSearchFailures[0].getCause())[0];
                        logger.debug(() -> format("%s shards failed for phase: [%s]", numShardFailures, currentPhase), cause);
                    }
                    onPhaseFailure(currentPhase, "Partial shards failure", null);
                } else {
                    int discrepancy = numShards - successfulOps.get();
                    assert discrepancy > 0 : "discrepancy: " + discrepancy;
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            "Partial shards failure (unavailable: {}, successful: {}, skipped: {}, num-shards: {}, phase: {})",
                            discrepancy,
                            successfulOps.get(),
                            skippedOps.get(),
                            numShards,
                            currentPhase
                        );
                    }
                    onPhaseFailure(currentPhase, "Partial shards failure (" + discrepancy + " shards unavailable)", null);
                }
                return;
            }
            var nextPhase = nextPhaseSupplier.get();
            if (logger.isTraceEnabled()) {
                final String resultsFrom = results.getSuccessfulResults()
                    .map(r -> r.getSearchShardTarget().toString())
                    .collect(Collectors.joining(","));
                logger.trace(
                    "[{}] Moving to next phase: [{}], based on results from: {} (cluster state version: {})",
                    currentPhase,
                    nextPhase.getName(),
                    resultsFrom,
                    clusterStateVersion
                );
            }
            try {
                nextPhase.run();
            } catch (RuntimeException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug(() -> format("Failed to execute [%s] while moving to [%s] phase", request, nextPhase.getName()), e);
                }
                onPhaseFailure(nextPhase.getName(), "", e);
            }
        }
    }

    private void onShardFailure(final int shardIndex, SearchShardTarget shard, final SearchShardIterator shardIt, Exception e) {
        // we always add the shard failure for a specific shard instance
        // we do make sure to clean it on a successful response from a shard
        onShardFailure(shardIndex, shard, e);
        final SearchShardTarget nextShard = shardIt.nextOrNull();
        final boolean lastShard = nextShard == null;
        logger.debug(() -> format("%s: Failed to execute [%s] lastShard [%s]", shard, request, lastShard), e);
        if (lastShard) {
            if (request.allowPartialSearchResults() == false) {
                if (requestCancelled.compareAndSet(false, true)) {
                    try {
                        searchTransportService.cancelSearchTask(
                            task.getId(),
                            "partial results are not allowed and at least one shard has failed"
                        );
                    } catch (Exception cancelFailure) {
                        logger.debug("Failed to cancel search request", cancelFailure);
                    }
                }
            }
            onShardGroupFailure(shardIndex, shard, e);
        }
        final int totalOps = this.totalOps.incrementAndGet();
        if (totalOps == expectedTotalOps) {
            onPhaseDone();
        } else if (totalOps > expectedTotalOps) {
            throw new AssertionError(
                "unexpected higher total ops [" + totalOps + "] compared to expected [" + expectedTotalOps + "]",
                new SearchPhaseExecutionException(name, "Shard failures", null, buildShardFailures(shardFailures))
            );
        } else {
            if (lastShard == false) {
                performPhaseOnShard(shardIndex, shardIt, nextShard);
            }
        }
    }

    /**
     * Executed once for every {@link ShardId} that failed on all available shard routing.
     *
     * @param shardIndex the shard index that failed
     * @param shardTarget the last shard target for this failure
     * @param exc the last failure reason
     */
    protected void onShardGroupFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {}

    /**
     * Executed once for every failed shard level request. This method is invoked before the next replica is tried for the given
     * shard target.
     * @param shardIndex the internal index for this shard. Each shard has an index / ordinal assigned that is used to reference
     *                   it's results
     * @param shardTarget the shard target for this failure
     * @param e the failure reason
     */
    @Override
    public void onShardFailure(final int shardIndex, SearchShardTarget shardTarget, Exception e) {
        if (TransportActions.isShardNotAvailableException(e)) {
            // Groups shard not available exceptions under a generic exception that returns a SERVICE_UNAVAILABLE(503)
            // temporary error.
            e = NoShardAvailableActionException.forOnShardFailureWrapper(e.getMessage());
        }
        // we don't aggregate shard on failures due to the internal cancellation,
        // but do keep the header counts right
        if ((requestCancelled.get() && isTaskCancelledException(e)) == false) {
            AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures.get();
            // lazily create shard failures, so we can early build the empty shard failure list in most cases (no failures)
            if (shardFailures == null) { // this is double checked locking but it's fine since SetOnce uses a volatile read internally
                synchronized (shardFailuresMutex) {
                    shardFailures = this.shardFailures.get(); // read again otherwise somebody else has created it?
                    if (shardFailures == null) { // still null so we are the first and create a new instance
                        shardFailures = new AtomicArray<>(results.getNumShards());
                        this.shardFailures.set(shardFailures);
                    }
                }
            }
            ShardSearchFailure failure = shardFailures.get(shardIndex);
            if (failure == null) {
                shardFailures.set(shardIndex, new ShardSearchFailure(e, shardTarget));
            } else {
                // the failure is already present, try and not override it with an exception that is less meaningless
                // for example, getting illegal shard state
                if (TransportActions.isReadOverrideException(e) && (e instanceof SearchContextMissingException == false)) {
                    shardFailures.set(shardIndex, new ShardSearchFailure(e, shardTarget));
                }
            }

            if (results.hasResult(shardIndex)) {
                assert failure == null : "shard failed before but shouldn't: " + failure;
                successfulOps.decrementAndGet(); // if this shard was successful before (initial phase) we have to adjust the counter
            }
        }
    }

    static boolean isTaskCancelledException(Exception e) {
        return ExceptionsHelper.unwrapCausesAndSuppressed(e, ex -> ex instanceof TaskCancelledException).isPresent();
    }

    /**
     * Executed once for every successful shard level request.
     * @param result the result returned form the shard
     * @param shardIt the shard iterator
     */
    protected void onShardResult(Result result, SearchShardIterator shardIt) {
        assert result.getShardIndex() != -1 : "shard index is not set";
        assert result.getSearchShardTarget() != null : "search shard target must not be null";
        hasShardResponse.set(true);
        if (logger.isTraceEnabled()) {
            logger.trace("got first-phase result from {}", result != null ? result.getSearchShardTarget() : null);
        }
        results.consumeResult(result, () -> onShardResultConsumed(result, shardIt));
    }

    private void onShardResultConsumed(Result result, SearchShardIterator shardIt) {
        successfulOps.incrementAndGet();
        // clean a previous error on this shard group (note, this code will be serialized on the same shardIndex value level
        // so its ok concurrency wise to miss potentially the shard failures being created because of another failure
        // in the #addShardFailure, because by definition, it will happen on *another* shardIndex
        AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures.get();
        if (shardFailures != null) {
            shardFailures.set(result.getShardIndex(), null);
        }
        // we need to increment successful ops first before we compare the exit condition otherwise if we
        // are fast we could concurrently update totalOps but then preempt one of the threads which can
        // cause the successor to read a wrong value from successfulOps if second phase is very fast ie. count etc.
        // increment all the "future" shards to update the total ops since we some may work and some may not...
        // and when that happens, we break on total ops, so we must maintain them
        successfulShardExecution(shardIt);
    }

    private void successfulShardExecution(SearchShardIterator shardsIt) {
        final int remainingOpsOnIterator;
        if (shardsIt.skip()) {
            // It's possible that we're skipping a shard that's unavailable
            // but its range was available in the IndexMetadata, in that
            // case the shardsIt.remaining() would be 0, expectedTotalOps
            // accounts for unavailable shards too.
            remainingOpsOnIterator = Math.max(shardsIt.remaining(), 1);
        } else {
            remainingOpsOnIterator = shardsIt.remaining() + 1;
        }
        final int xTotalOps = totalOps.addAndGet(remainingOpsOnIterator);
        if (xTotalOps == expectedTotalOps) {
            onPhaseDone();
        } else if (xTotalOps > expectedTotalOps) {
            throw new AssertionError(
                "unexpected higher total ops [" + xTotalOps + "] compared to expected [" + expectedTotalOps + "]",
                new SearchPhaseExecutionException(name, "Shard failures", null, buildShardFailures(shardFailures))
            );
        }
    }

    /**
      * Returns the currently executing search task
    */
    public final SearchTask getTask() {
        return task;
    }

    /**
      * Returns the currently executing search request
      */
    public final SearchRequest getRequest() {
        return request;
    }

    /**
      * Returns the targeted {@link OriginalIndices} for the provided {@code shardIndex}.
      */
    public OriginalIndices getOriginalIndices(int shardIndex) {
        return shardIterators[shardIndex].getOriginalIndices();
    }

    private SearchResponse buildSearchResponse(
        SearchResponseSections internalSearchResponse,
        ShardSearchFailure[] failures,
        String scrollId,
        BytesReference searchContextId
    ) {
        int numSuccess = successfulOps.get();
        int numFailures = failures.length;
        final int numShards = results.getNumShards();
        assert numSuccess + numFailures == numShards
            : "numSuccess(" + numSuccess + ") + numFailures(" + numFailures + ") != totalShards(" + numShards + ")";
        return new SearchResponse(
            internalSearchResponse,
            scrollId,
            numShards,
            numSuccess,
            skippedOps.get(),
            buildTookInMillis(),
            failures,
            clusters,
            searchContextId
        );
    }

    boolean buildPointInTimeFromSearchResults() {
        return false;
    }

    /**
      * Builds and sends the final search response back to the user.
      *
      * @param internalSearchResponse the internal search response
      * @param queryResults           the results of the query phase
      */
    public void sendSearchResponse(SearchResponseSections internalSearchResponse, AtomicArray<? extends SearchPhaseResult> queryResults) {
        ShardSearchFailure[] failures = buildShardFailures(shardFailures);
        Boolean allowPartialResults = request.allowPartialSearchResults();
        assert allowPartialResults != null : "SearchRequest missing setting for allowPartialSearchResults";
        if (allowPartialResults == false && failures.length > 0) {
            raisePhaseFailure(new SearchPhaseExecutionException("", "Shard failures", null, failures));
        } else {
            final String scrollId = request.scroll() != null ? TransportSearchHelper.buildScrollId(queryResults) : null;
            final BytesReference searchContextId;
            if (buildPointInTimeFromSearchResults()) {
                searchContextId = SearchContextId.encode(queryResults.asList(), aliasFilter, minTransportVersion, failures);
            } else {
                if (request.source() != null
                    && request.source().pointInTimeBuilder() != null
                    && request.source().pointInTimeBuilder().singleSession() == false) {
                    searchContextId = request.source().pointInTimeBuilder().getEncodedId();
                } else {
                    searchContextId = null;
                }
            }
            ActionListener.respondAndRelease(listener, buildSearchResponse(internalSearchResponse, failures, scrollId, searchContextId));
        }
    }

    /**
     * This method will communicate a fatal phase failure back to the user. In contrast to a shard failure
     * will this method immediately fail the search request and return the failure to the issuer of the request
     * @param phase the phase that failed
     * @param msg an optional message
     * @param cause the cause of the phase failure
     */
    @Override
    public void onPhaseFailure(String phase, String msg, Throwable cause) {
        raisePhaseFailure(new SearchPhaseExecutionException(phase, msg, cause, buildShardFailures(shardFailures)));
    }

    /**
      * Releases a search context with the given context ID on the node the given connection is connected to.
      * @see org.elasticsearch.search.query.QuerySearchResult#getContextId()
      * @see org.elasticsearch.search.fetch.FetchSearchResult#getContextId()
      *
      */
    @Override
    public void sendReleaseSearchContext(ShardSearchContextId contextId, Transport.Connection connection) {
        assert isPartOfPointInTime(contextId) == false : "Must not release point in time context [" + contextId + "]";
        if (connection != null) {
            searchTransportService.sendFreeContext(connection, contextId, ActionListener.noop());
        }
    }

    /**
     * Executed once all shard results have been received and processed
     * @see #onShardFailure(int, SearchShardTarget, Exception)
     * @see #onShardResult(SearchPhaseResult, SearchShardIterator)
     */
    private void onPhaseDone() {  // as a tribute to @kimchy aka. finishHim()
        executeNextPhase(name, this::getNextPhase);
    }

    /**
     * Returns a connection to the node if connected otherwise and {@link org.elasticsearch.transport.ConnectTransportException} will be
     * thrown.
     */
    @Override
    public final Transport.Connection getConnection(String clusterAlias, String nodeId) {
        return nodeIdToConnection.apply(clusterAlias, nodeId);
    }

    /**
     * Returns the {@link SearchTransportService} to send shard request to other nodes
     */
    public SearchTransportService getSearchTransport() {
        return searchTransportService;
    }

    public final void execute(Runnable command) {
        executor.execute(command);
    }

    /**
     * Builds an request for the initial search phase.
     *
     * @param shardIt the target {@link SearchShardIterator}
     * @param shardIndex the index of the shard that is used in the coordinator node to
     *                   tiebreak results with identical sort values
     */
    protected final ShardSearchRequest buildShardSearchRequest(SearchShardIterator shardIt, int shardIndex) {
        AliasFilter filter = aliasFilter.get(shardIt.shardId().getIndex().getUUID());
        assert filter != null;
        float indexBoost = concreteIndexBoosts.getOrDefault(shardIt.shardId().getIndex().getUUID(), DEFAULT_INDEX_BOOST);
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            shardIt.getOriginalIndices(),
            request,
            shardIt.shardId(),
            shardIndex,
            results.getNumShards(),
            filter,
            indexBoost,
            timeProvider.absoluteStartMillis(),
            shardIt.getClusterAlias(),
            shardIt.getSearchContextId(),
            shardIt.getSearchContextKeepAlive()
        );
        // if we already received a search result we can inform the shard that it
        // can return a null response if the request rewrites to match none rather
        // than creating an empty response in the search thread pool.
        // Note that, we have to disable this shortcut for queries that create a context (scroll and search context).
        shardRequest.canReturnNullResponseIfMatchNoDocs(hasShardResponse.get() && shardRequest.scroll() == null);
        return shardRequest;
    }

    /**
     * Returns the next phase based on the results of the initial search phase
     */
    protected abstract SearchPhase getNextPhase();

    static final class PendingExecutions {
        private final Semaphore semaphore;
        private final ConcurrentLinkedQueue<Consumer<Releasable>> queue = new ConcurrentLinkedQueue<>();

        PendingExecutions(int permits) {
            assert permits > 0 : "not enough permits: " + permits;
            semaphore = new Semaphore(permits);
        }

        void submit(Consumer<Releasable> task) {
            if (semaphore.tryAcquire()) {
                executeAndRelease(task);
            } else {
                queue.add(task);
                if (semaphore.tryAcquire()) {
                    task = pollNextTaskOrReleasePermit();
                    if (task != null) {
                        executeAndRelease(task);
                    }
                }
            }
        }

        private void executeAndRelease(Consumer<Releasable> task) {
            do {
                final SubscribableListener<Void> onDone = new SubscribableListener<>();
                task.accept(() -> onDone.onResponse(null));
                if (onDone.isDone()) {
                    // keep going on the current thread, no need to fork
                    task = pollNextTaskOrReleasePermit();
                } else {
                    onDone.addListener(new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {
                            final Consumer<Releasable> nextTask = pollNextTaskOrReleasePermit();
                            if (nextTask != null) {
                                executeAndRelease(nextTask);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assert false : e;
                        }
                    });
                    return;
                }
            } while (task != null);
        }

        private Consumer<Releasable> pollNextTaskOrReleasePermit() {
            var task = queue.poll();
            if (task == null) {
                semaphore.release();
                while (queue.peek() != null && semaphore.tryAcquire()) {
                    task = queue.poll();
                    if (task == null) {
                        semaphore.release();
                    } else {
                        return task;
                    }
                }
            }
            return task;
        }
    }
}
