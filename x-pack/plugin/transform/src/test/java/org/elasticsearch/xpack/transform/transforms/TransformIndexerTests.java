/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.checkpoint.MockTimebasedCheckpointProvider;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.InMemoryTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.transform.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests.randomPivotConfig;
import static org.mockito.Mockito.mock;

public class TransformIndexerTests extends ESTestCase {

    private static final SearchResponse ONE_HIT_SEARCH_RESPONSE = new SearchResponse(
        new InternalSearchResponse(
            new SearchHits(new SearchHit[] { new SearchHit(1) }, new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1.0f),
            // Simulate completely null aggs
            null,
            new Suggest(Collections.emptyList()),
            new SearchProfileShardResults(Collections.emptyMap()),
            false,
            false,
            1
        ),
        "",
        1,
        1,
        0,
        0,
        ShardSearchFailure.EMPTY_ARRAY,
        SearchResponse.Clusters.EMPTY
    );

    private Client client;
    private ThreadPool threadPool;
    private TransformAuditor auditor;
    private TransformConfigManager transformConfigManager;

    class MockedTransformIndexer extends TransformIndexer {

        private final ThreadPool threadPool;

        private int deleteByQueryCallCount = 0;
        // used for synchronizing with the test
        private CountDownLatch searchLatch;
        private CountDownLatch doProcessLatch;

        // how many loops to execute until reporting done
        private int numberOfLoops;

        MockedTransformIndexer(
            int numberOfLoops,
            ThreadPool threadPool,
            TransformConfigManager transformsConfigManager,
            CheckpointProvider checkpointProvider,
            TransformAuditor auditor,
            TransformConfig transformConfig,
            Map<String, String> fieldMappings,
            AtomicReference<IndexerState> initialState,
            TransformIndexerPosition initialPosition,
            TransformIndexerStats jobStats,
            TransformContext context
        ) {
            super(
                threadPool,
                transformsConfigManager,
                checkpointProvider,
                auditor,
                transformConfig,
                fieldMappings,
                initialState,
                initialPosition,
                jobStats,
                /* TransformProgress */ null,
                TransformCheckpoint.EMPTY,
                TransformCheckpoint.EMPTY,
                context
            );
            this.threadPool = threadPool;
            this.numberOfLoops = numberOfLoops;
        }

        public void initialize() {
            this.initializeFunction();
        }

        public CountDownLatch createAwaitForSearchLatch(int count) {
            return searchLatch = new CountDownLatch(count);
        }

        public CountDownLatch createCountDownOnResponseLatch(int count) {
            return doProcessLatch = new CountDownLatch(count);
        }

        @Override
        void doGetInitialProgress(SearchRequest request, ActionListener<SearchResponse> responseListener) {
            responseListener.onResponse(ONE_HIT_SEARCH_RESPONSE);
        }

        @Override
        void doDeleteByQuery(DeleteByQueryRequest deleteByQueryRequest, ActionListener<BulkByScrollResponse> responseListener) {
            deleteByQueryCallCount++;
            responseListener.onResponse(
                new BulkByScrollResponse(
                    TimeValue.ZERO,
                    new BulkByScrollTask.Status(Collections.emptyList(), null),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    false
                )
            );
        }

        @Override
        void refreshDestinationIndex(RefreshRequest refreshRequest, ActionListener<RefreshResponse> responseListener) {
            responseListener.onResponse(new RefreshResponse(1, 1, 0, Collections.emptyList()));
        }

        @Override
        protected void doNextSearch(long waitTimeInNanos, ActionListener<SearchResponse> nextPhase) {
            if (searchLatch != null) {
                try {
                    searchLatch.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> nextPhase.onResponse(ONE_HIT_SEARCH_RESPONSE));
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            if (doProcessLatch != null) {
                doProcessLatch.countDown();
            }
            threadPool.executor(ThreadPool.Names.GENERIC)
                .execute(() -> nextPhase.onResponse(new BulkResponse(new BulkItemResponse[0], 100)));
        }

        @Override
        protected void doSaveState(IndexerState state, TransformIndexerPosition position, Runnable next) {
            assert state == IndexerState.STARTED || state == IndexerState.INDEXING || state == IndexerState.STOPPED;
            next.run();
        }

        @Override
        protected IterationResult<TransformIndexerPosition> doProcess(SearchResponse searchResponse) {
            assert numberOfLoops > 0;
            --numberOfLoops;
            // pretend that we processed 10k documents for each call
            getStats().incrementNumDocuments(10_000);
            return new IterationResult<>(
                Collections.singletonList(new IndexRequest()),
                new TransformIndexerPosition(null, null),
                numberOfLoops == 0
            );
        }

        public boolean waitingForNextSearch() {
            return super.getScheduledNextSearch() != null;
        }

        public int getDeleteByQueryCallCount() {
            return deleteByQueryCallCount;
        }
    }

    @Before
    public void setUpMocks() {
        auditor = MockTransformAuditor.createMockAuditor();
        transformConfigManager = new InMemoryTransformConfigManager();
        client = new NoOpClient(getTestName());
        threadPool = new TestThreadPool(ThreadPool.Names.GENERIC);
    }

    @After
    public void tearDownClient() {
        client.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testRetentionPolicyExecution() throws Exception {
        TransformConfig config = new TransformConfig(
            randomAlphaOfLength(10),
            randomSourceConfig(),
            randomDestConfig(),
            null,
            new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)),
            null,
            randomPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            null,
            TimeRetentionPolicyConfigTests.randomTimeRetentionPolicyConfig(),
            null,
            null
        );
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STARTED);
        {
            TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
            final MockedTransformIndexer indexer = createMockIndexer(
                10,
                config,
                state,
                null,
                threadPool,
                auditor,
                new TransformIndexerStats(),
                context
            );

            indexer.start();
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertEquals(indexer.getState(), IndexerState.INDEXING);

            assertBusy(() -> assertEquals(1L, indexer.getLastCheckpoint().getCheckpoint()), 5, TimeUnit.HOURS);

            // delete by query has been executed
            assertEquals(1, indexer.getDeleteByQueryCallCount());
        }

        // test without retention
        config = new TransformConfig(
            randomAlphaOfLength(10),
            randomSourceConfig(),
            randomDestConfig(),
            null,
            new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)),
            null,
            randomPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            null,
            null,
            null,
            null
        );

        state = new AtomicReference<>(IndexerState.STARTED);
        {
            TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
            final MockedTransformIndexer indexer = createMockIndexer(
                10,
                config,
                state,
                null,
                threadPool,
                auditor,
                new TransformIndexerStats(),
                context
            );

            indexer.start();
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertEquals(indexer.getState(), IndexerState.INDEXING);

            assertBusy(() -> assertEquals(1L, indexer.getLastCheckpoint().getCheckpoint()), 5, TimeUnit.SECONDS);

            // delete by query has _not_ been executed
            assertEquals(0, indexer.getDeleteByQueryCallCount());
        }
    }

    private MockedTransformIndexer createMockIndexer(
        int numberOfLoops,
        TransformConfig config,
        AtomicReference<IndexerState> state,
        Consumer<String> failureConsumer,
        ThreadPool threadPool,
        TransformAuditor auditor,
        TransformIndexerStats jobStats,
        TransformContext context
    ) {
        CheckpointProvider checkpointProvider = new MockTimebasedCheckpointProvider(config);
        transformConfigManager.putTransformConfiguration(config, ActionListener.wrap(r -> {}, e -> {}));

        MockedTransformIndexer indexer = new MockedTransformIndexer(
            numberOfLoops,
            threadPool,
            transformConfigManager,
            checkpointProvider,
            auditor,
            config,
            Collections.emptyMap(),
            state,
            null,
            jobStats,
            context
        );

        indexer.initialize();
        return indexer;
    }
}
