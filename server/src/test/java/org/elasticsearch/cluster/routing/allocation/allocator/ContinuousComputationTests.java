/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.sameInstance;

public class ContinuousComputationTests extends ESTestCase {

    private static ThreadPool threadPool;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool("test");
    }

    @AfterClass
    public static void terminateThreadPool() {
        try {
            assertTrue(ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        } finally {
            threadPool = null;
        }
    }

    public void testConcurrency() throws Exception {

        final var computation = new ContinuousComputation<Integer>(threadPool.generic()) {

            public final Semaphore executePermit = new Semaphore(1);
            public final AtomicInteger last = new AtomicInteger();
            public final AtomicInteger count = new AtomicInteger();

            @Override
            protected void processInput(Integer input) {
                assertThat("Should execute computations sequentially", executePermit.tryAcquire(1), equalTo(true));
                assertThat("Should not reorder executions", input, greaterThan(last.getAndSet(input)));
                count.incrementAndGet();
                executePermit.release();
            }
        };

        final AtomicInteger listenersComputed = new AtomicInteger();
        final AtomicInteger inputGenerator = new AtomicInteger(0);
        final int inputs = 10_000;

        for (int i = 0; i < inputs; i++) {
            final int input = inputGenerator.incrementAndGet();
            computation.onNewInput(input, () -> {
                assertThat("Should execute listener after the computation is complete",
                    computation.last.get(), greaterThanOrEqualTo(input));
                listenersComputed.incrementAndGet();
            });
        }

        assertBusy(() -> assertFalse(computation.isActive()));

        assertThat("Should keep the latest result", computation.last.get(), equalTo(inputGenerator.get()));
        assertThat("May skip some computations", computation.count.get(), lessThan(inputs));
        assertBusy(() -> assertThat("Should complete all listeners", listenersComputed.get(), equalTo(inputs)));
    }

    public void testSkipsObsoleteValues() throws Exception {
        final var barrier = new CyclicBarrier(2);
        final Runnable await = () -> {
            try {
                barrier.await(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        };

        final var initialInput = new Object();
        final var becomesStaleInput = new Object();
        final var skippedInput = new Object();
        final var finalInput = new Object();

        final var result = new AtomicReference<Object>();
        final var computation = new ContinuousComputation<Object>(threadPool.generic()) {
            @Override
            protected void processInput(Object input) {
                assertNotEquals(input, skippedInput);
                await.run();
                result.set(input);
                await.run();
                // becomesStaleInput should have become stale by now, but other inputs should remain fresh
                assertEquals(isFresh(input), input != becomesStaleInput);
                await.run();
            }
        };

        computation.onNewInput(initialInput);
        await.run();
        assertTrue(computation.isActive());
        await.run();
        assertThat(result.get(), sameInstance(initialInput));
        await.run();
        assertBusy(() -> assertFalse(computation.isActive()));

        computation.onNewInput(becomesStaleInput); // triggers a computation
        await.run();
        assertTrue(computation.isActive());

        computation.onNewInput(skippedInput); // obsoleted by computation 4 before computation 2 is finished, so skipped
        computation.onNewInput(finalInput); // triggers a computation once 2 is finished

        await.run();
        await.run();
        assertThat(result.get(), equalTo(becomesStaleInput));
        assertTrue(computation.isActive());

        await.run();
        assertTrue(computation.isActive());
        await.run();
        assertThat(result.get(), equalTo(finalInput));
        await.run();
        assertBusy(() -> assertFalse(computation.isActive()));
    }
}
