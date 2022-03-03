/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class MetadataIndexStateServiceBatchingTests extends ESSingleNodeTestCase {

    /*
     * This class is painfully clever and knows all sorts of implementation details about MetadataIndexStateService. It's hard to get
     * around that, as we want to make sure that *batching* of opens and closes and adding index blocks will work as expected.
     *
     * In the case of opens that's easy enough, we just need to get hands-on enough to make sure that the open tasks are executed in a
     * single batch -- we do that by blocking up the master service's tasks queue, then running the opens (so they are added to the queue),
     * and then unblocking the master service again so everything processes in a single batch.
     *
     * But closing and adding blocks are both two-step processes where the second step is itself another cluster state update task, and we
     * want to make sure that those batch up correctly, too. So there we run the same trick as with opens, but twice: we block the
     * master service in order to batch up the initial tasks, then block it again, so that when the first tasks enqueue the second tasks
     * they're doing so into a blocked master service, finally the master service is unblocked once more so that the second tasks process
     * together in a batch.
     */

    public void testBatchOpenIndices() throws Exception {
        final var masterService = getInstanceFromNode(ClusterService.class).getMasterService();

        // create some indices
        createIndex("test-1", client().admin().indices().prepareCreate("test-1"));
        createIndex("test-2", client().admin().indices().prepareCreate("test-2"));
        createIndex("test-3", client().admin().indices().prepareCreate("test-3"));
        ensureGreen("test-1", "test-2", "test-3");

        final var block1 = blockMasterService(masterService);
        block1.call(); // wait for block

        // fire off some opens
        final var future1 = client().admin().indices().prepareOpen("test-1").execute();
        final var future2 = client().admin().indices().prepareOpen("test-2", "test-3").execute();

        // check the queue for the open-indices tasks
        assertThat(masterService.pendingTasks(), hasSize(3)); // two plus the blocking task itself

        block1.call(); // release block

        // assert that the requests were acknowledged
        assertAcked(future1.get());
        assertAcked(future2.get());
    }

    public void testBatchCloseIndices() throws Exception {
        final var masterService = getInstanceFromNode(ClusterService.class).getMasterService();

        // create some indices
        createIndex("test-1", client().admin().indices().prepareCreate("test-1"));
        createIndex("test-2", client().admin().indices().prepareCreate("test-2"));
        createIndex("test-3", client().admin().indices().prepareCreate("test-3"));
        ensureGreen("test-1", "test-2", "test-3");

        final var block1 = blockMasterService(masterService);
        block1.call(); // wait for block

        // fire off some closes
        final var future1 = client().admin().indices().prepareClose("test-1").execute();
        final var future2 = client().admin().indices().prepareClose("test-2", "test-3").execute();

        // check the queue for the first close tasks (the add-block-index-to-close tasks)
        assertThat(masterService.pendingTasks(), hasSize(3)); // two plus the blocking task itself

        // add *another* block to the end of the pending tasks, then unblock the current block so we can progress,
        // then immediately block again on that new block
        final var block2 = blockMasterService(masterService);
        block1.call(); // release block
        block2.call(); // wait for block

        // wait for the queue to have the second close tasks (the close-indices tasks)
        assertBusy(() -> assertThat(masterService.pendingTasks(), hasSize(3))); // two plus the blocking task itself

        block2.call(); // release block

        // assert that the requests were acknowledged
        final var resp1 = future1.get();
        assertAcked(resp1);
        assertThat(resp1.getIndices(), hasSize(1));
        assertThat(resp1.getIndices().get(0).getIndex().getName(), is("test-1"));

        final var resp2 = future2.get();
        assertAcked(resp2);
        assertThat(resp2.getIndices(), hasSize(2));
        assertThat(resp2.getIndices().stream().map(r -> r.getIndex().getName()).toList(), containsInAnyOrder("test-2", "test-3"));
    }

    public void testBatchBlockIndices() throws Exception {
        final var masterService = getInstanceFromNode(ClusterService.class).getMasterService();

        // create some indices
        createIndex("test-1", client().admin().indices().prepareCreate("test-1"));
        createIndex("test-2", client().admin().indices().prepareCreate("test-2"));
        createIndex("test-3", client().admin().indices().prepareCreate("test-3"));
        ensureGreen("test-1", "test-2", "test-3");

        final var block1 = blockMasterService(masterService);
        block1.call(); // wait for block

        // fire off some closes
        final var future1 = client().admin().indices().prepareAddBlock(APIBlock.WRITE, "test-1").execute();
        final var future2 = client().admin().indices().prepareAddBlock(APIBlock.WRITE, "test-2", "test-3").execute();

        // check the queue for the first add-block tasks (the add-index-block tasks)
        assertThat(masterService.pendingTasks(), hasSize(3)); // two plus the blocking task itself

        // add *another* block to the end of the pending tasks, then unblock the current block so we can progress,
        // then immediately block again on that new block
        final var block2 = blockMasterService(masterService);
        block1.call(); // release block
        block2.call(); // wait for block

        // wait for the queue to have the second add-block tasks (the finalize-index-block tasks)
        assertBusy(() -> assertThat(masterService.pendingTasks(), hasSize(3))); // two plus the blocking task itself

        block2.call(); // release block

        // assert that the requests were acknowledged
        final var resp1 = future1.get();
        assertAcked(resp1);
        assertThat(resp1.getIndices(), hasSize(1));
        assertThat(resp1.getIndices().get(0).getIndex().getName(), is("test-1"));

        final var resp2 = future2.get();
        assertAcked(resp2);
        assertThat(resp2.getIndices(), hasSize(2));
        assertThat(resp2.getIndices().stream().map(r -> r.getIndex().getName()).toList(), containsInAnyOrder("test-2", "test-3"));
    }

    private static Callable<Void> blockMasterService(MasterService masterService) {
        final var executionBarrier = new CyclicBarrier(2);
        masterService.submitStateUpdateTask(
            "block",
            new ExpectSuccessTask(),
            ClusterStateTaskConfig.build(Priority.URGENT),
            (currentState, taskContexts) -> {
                executionBarrier.await(10, TimeUnit.SECONDS); // notify test thread that the master service is blocked
                executionBarrier.await(10, TimeUnit.SECONDS); // wait for test thread to release us
                for (final var taskContext : taskContexts) {
                    taskContext.success(EXPECT_SUCCESS_LISTENER);
                }
                return currentState;
            }
        );

        return () -> {
            executionBarrier.await(10, TimeUnit.SECONDS);
            return null;
        };
    }

    /**
     * Listener that asserts it does not fail.
     */
    private static final ActionListener<ClusterState> EXPECT_SUCCESS_LISTENER = new ActionListener<>() {
        @Override
        public void onResponse(ClusterState clusterState) {}

        @Override
        public void onFailure(Exception e) {
            throw new AssertionError("should not be called", e);
        }
    };

    /**
     * Task that asserts it does not fail.
     */
    private static class ExpectSuccessTask implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            throw new AssertionError("should not be called", e);
        }

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            // see parent method javadoc, we use dedicated listeners rather than calling this method
            throw new AssertionError("should not be called");
        }
    }
}
