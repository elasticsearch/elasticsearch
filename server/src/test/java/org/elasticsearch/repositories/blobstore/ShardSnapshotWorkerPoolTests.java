/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

@TestLogging(value = "org.elasticsearch.repositories.blobstore:TRACE", reason = "debugging")
public class ShardSnapshotWorkerPoolTests extends ESTestCase {

    private static ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdownThreadPool() {
        TestThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    private class MockedRepo {
        private final AtomicInteger expectedUploads = new AtomicInteger();
        private final AtomicInteger finishedUploads = new AtomicInteger();
        private final AtomicInteger finishedSnapshots = new AtomicInteger();
        private final CountDownLatch uploadBlocker;
        private final CountDownLatch snapshotBlocker;
        private final IntSupplier fileUploadSupplier;
        private ShardSnapshotWorkerPool workers;

        MockedRepo(IntSupplier fileUploadSupplier) {
            this(new CountDownLatch(0), new CountDownLatch(0), fileUploadSupplier);
        }

        MockedRepo(CountDownLatch uploadBlocker, CountDownLatch snapshotBlocker, IntSupplier fileUploadSupplier) {
            this.uploadBlocker = uploadBlocker;
            this.snapshotBlocker = snapshotBlocker;
            this.fileUploadSupplier = fileUploadSupplier;
        }

        public void setWorkers(ShardSnapshotWorkerPool workers) {
            this.workers = workers;
        }

        public void snapshotShard(SnapshotShardContext context) {
            try {
                snapshotBlocker.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            int filesToUpload = fileUploadSupplier.getAsInt();
            if (filesToUpload == 0) {
                finishedSnapshots.incrementAndGet();
                return;
            }
            expectedUploads.addAndGet(filesToUpload);
            ActionListener<Void> uploadListener = new GroupedActionListener<>(
                ActionListener.wrap(finishedSnapshots::incrementAndGet),
                filesToUpload
            );
            for (int i = 0; i < filesToUpload; i++) {
                workers.enqueueFileUpload(new ShardSnapshotWorkerPool.SnapshotFileUpload(context, createMockedFileInfo(), uploadListener));
            }
        }

        public void uploadFile(SnapshotShardContext context, BlobStoreIndexShardSnapshot.FileInfo fileInfo) {
            try {
                uploadBlocker.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            finishedUploads.incrementAndGet();
        }

        public int expectedUploads() {
            return expectedUploads.get();
        }

        public int finishedUploads() {
            return finishedUploads.get();
        }

        public int finishedSnapshots() {
            return finishedSnapshots.get();
        }
    }

    private BlobStoreIndexShardSnapshot.FileInfo createMockedFileInfo() {
        String filename = randomAlphaOfLength(10);
        return new BlobStoreIndexShardSnapshot.FileInfo(filename, mock(StoreFileMetadata.class), null);
    }

    public void testAllWorkersExitWhenBothQueuesAreExhausted() throws Exception {
        Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        int maxSize = randomIntBetween(1, threadPool.info(ThreadPool.Names.SNAPSHOT).getMax());
        MockedRepo repo = new MockedRepo(() -> randomIntBetween(0, 10));
        ShardSnapshotWorkerPool workers = new ShardSnapshotWorkerPool(maxSize, executor, repo::snapshotShard, repo::uploadFile);
        assertThat(workers.size(), equalTo(0));
        repo.setWorkers(workers);
        int shardsToSnapshot = randomIntBetween(1, 100);
        for (int i = 0; i < shardsToSnapshot; i++) {
            workers.enqueueShardSnapshot(mock(SnapshotShardContext.class));
        }
        assertBusy(() -> assertThat(repo.finishedSnapshots(), equalTo(shardsToSnapshot)));
        assertBusy(() -> assertThat(workers.size(), equalTo(0)));
        assertBusy(() -> assertThat(repo.finishedUploads(), equalTo(repo.expectedUploads())));
    }

    public void testExitingWorkerCreatesNewWorkersIfNecessary() throws Exception {
        Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        int maxSize = randomIntBetween(1, threadPool.info(ThreadPool.Names.SNAPSHOT).getMax());
        CountDownLatch uploadBlocker = new CountDownLatch(1);
        CountDownLatch snapshotBlocker = new CountDownLatch(1);
        MockedRepo repo = new MockedRepo(uploadBlocker, snapshotBlocker, () -> 1); // Each shard snapshot results in one file snapshot
        ShardSnapshotWorkerPool workers = new ShardSnapshotWorkerPool(maxSize, executor, repo::snapshotShard, repo::uploadFile);
        repo.setWorkers(workers);
        // Make sure everyone is busy
        int enqueuedSnapshots = maxSize;
        for (int i = 0; i < enqueuedSnapshots; i++) {
            workers.enqueueShardSnapshot(mock(SnapshotShardContext.class));
        }
        assertBusy(() -> assertThat(workers.size(), equalTo(maxSize)));
        snapshotBlocker.countDown(); // move all workers to file upload
        // Adding a new shard snapshot would not get any new worker
        enqueuedSnapshots += 1;
        workers.enqueueShardSnapshot(mock(SnapshotShardContext.class));
        assertThat(workers.size(), equalTo(maxSize));
        uploadBlocker.countDown();
        assertBusy(() -> assertThat(repo.finishedUploads(), equalTo(repo.expectedUploads())));
        assertBusy(() -> assertThat(workers.size(), equalTo(0)));
        assertThat(repo.finishedSnapshots(), equalTo(enqueuedSnapshots));
    }
}
