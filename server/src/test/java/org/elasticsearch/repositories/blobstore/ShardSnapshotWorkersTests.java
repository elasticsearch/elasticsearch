/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class ShardSnapshotWorkersTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static Executor executor;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool("test");
        executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
    }

    @After
    public void shutdownThreadPool() {
        TestThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    private static class MockedRepo {
        private final AtomicInteger expectedFileSnapshotTasks = new AtomicInteger();
        private final AtomicInteger finishedFileSnapshotTasks = new AtomicInteger();
        private final AtomicInteger finishedShardSnapshotTasks = new AtomicInteger();
        private final AtomicInteger finishedShardSnapshots = new AtomicInteger();
        private final CountDownLatch snapshotShardBlocker;
        private ShardSnapshotWorkers workers;

        MockedRepo(CountDownLatch snapshotShardBlocker) {
            this.snapshotShardBlocker = snapshotShardBlocker;
        }

        public void setWorkers(ShardSnapshotWorkers workers) {
            this.workers = workers;
        }

        public void snapshotShard(SnapshotShardContext context) {
            try {
                snapshotShardBlocker.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            int filesToUpload = randomIntBetween(0, 10);
            if (filesToUpload == 0) {
                finishedShardSnapshots.incrementAndGet();
                return;
            }
            expectedFileSnapshotTasks.addAndGet(filesToUpload);
            ActionListener<Void> uploadListener = new GroupedActionListener<>(
                ActionListener.wrap(finishedShardSnapshots::incrementAndGet),
                filesToUpload
            );
            for (int i = 0; i < filesToUpload; i++) {
                workers.enqueueFileUpload(context, createDummyFileInfo(), uploadListener);
            }
            finishedShardSnapshotTasks.incrementAndGet();
        }

        public void snapshotFile(SnapshotShardContext context, BlobStoreIndexShardSnapshot.FileInfo fileInfo) {
            finishedFileSnapshotTasks.incrementAndGet();
        }

        public int expectedFileSnapshotTasks() {
            return expectedFileSnapshotTasks.get();
        }

        public int finishedFileSnapshotTasks() {
            return finishedFileSnapshotTasks.get();
        }

        public int finishedShardSnapshots() {
            return finishedShardSnapshots.get();
        }
    }

    private static BlobStoreIndexShardSnapshot.FileInfo createDummyFileInfo() {
        String filename = randomAlphaOfLength(10);
        StoreFileMetadata metadata = new StoreFileMetadata(filename, 10, "CHECKSUM", Version.CURRENT.luceneVersion.toString());
        return new BlobStoreIndexShardSnapshot.FileInfo(filename, metadata, null);
    }

    private SnapshotShardContext createDummyContext() {
        return createDummyContext(new SnapshotId(randomAlphaOfLength(10), UUIDs.randomBase64UUID()));
    }

    private SnapshotShardContext createDummyContext(final SnapshotId snapshotId) {
        IndexId indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), 1);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder(indexId.getName()).settings(settings).build(),
            Settings.EMPTY
        );
        Store dummyStore = new Store(shardId, indexSettings, new ByteBuffersDirectory(), new DummyShardLock(shardId));
        return new SnapshotShardContext(
            dummyStore,
            null,
            snapshotId,
            indexId,
            new Engine.IndexCommitRef(null, () -> {}),
            null,
            IndexShardSnapshotStatus.newInitializing(null),
            Version.CURRENT,
            Collections.emptyMap(),
            ActionListener.noop()
        );
    }

    public void testEnqueueCreatesNewWorkersIfNecessary() throws Exception {
        int maxSize = randomIntBetween(1, threadPool.info(ThreadPool.Names.SNAPSHOT).getMax());
        CountDownLatch snapshotBlocker = new CountDownLatch(1);
        MockedRepo repo = new MockedRepo(snapshotBlocker);
        ShardSnapshotWorkers workers = new ShardSnapshotWorkers(maxSize, executor, repo::snapshotShard, repo::snapshotFile);
        repo.setWorkers(workers);
        int enqueuedSnapshots = maxSize - 1; // It's possible to create at least one more worker
        for (int i = 0; i < enqueuedSnapshots; i++) {
            workers.enqueueShardSnapshot(createDummyContext());
        }
        assertBusy(() -> assertThat(workers.size(), equalTo(maxSize - 1)));
        // Adding at least one new shard snapshot would create a new worker
        int newTasks = randomIntBetween(1, 10);
        for (int i = 0; i < newTasks; i++) {
            workers.enqueueShardSnapshot(createDummyContext());
        }
        enqueuedSnapshots += newTasks;
        assertThat(workers.size(), equalTo(maxSize));
        snapshotBlocker.countDown();
        // Eventually all workers exit
        assertBusy(() -> assertThat(workers.size(), equalTo(0)));
        assertThat(repo.finishedFileSnapshotTasks(), equalTo(repo.expectedFileSnapshotTasks()));
        assertThat(repo.finishedShardSnapshots(), equalTo(enqueuedSnapshots));
    }

    public void testCompareToShardSnapshotTask() {
        ShardSnapshotWorkers workers = new ShardSnapshotWorkers(1, executor, context -> {}, (context, fileInfo) -> {});
        SnapshotId s1 = new SnapshotId("a", UUIDs.randomBase64UUID());
        SnapshotId s2 = new SnapshotId("b", UUIDs.randomBase64UUID());
        SnapshotId s3 = new SnapshotId("a", UUIDs.randomBase64UUID());
        SnapshotShardContext s1Context = createDummyContext(s1);
        SnapshotShardContext s2Context = createDummyContext(s2);
        SnapshotShardContext s3Context = createDummyContext(s3);
        // Two tasks with the same snapshot name and of the same type have the same priority
        assertThat(workers.new ShardSnapshotTask(s1Context).compareTo(workers.new ShardSnapshotTask(s3Context)), equalTo(0));
        // Within the same snapshot, shard snapshot has higher priority over file snapshot
        assertThat(
            workers.new ShardSnapshotTask(s1Context).compareTo(
                workers.new FileSnapshotTask(s1Context, createDummyFileInfo(), ActionListener.noop())
            ),
            lessThan(0)
        );
        // Priority of task types matter only within the same snapshot
        assertThat(
            workers.new ShardSnapshotTask(s2Context).compareTo(
                workers.new FileSnapshotTask(s1Context, createDummyFileInfo(), ActionListener.noop())
            ),
            greaterThan(0)
        );
    }
}
