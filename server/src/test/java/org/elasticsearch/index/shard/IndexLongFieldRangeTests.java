/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.test.ESTestCase;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.index.shard.IndexLongFieldRangeTestUtils.randomSpecificRange;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class IndexLongFieldRangeTests extends ESTestCase {

    public void testMutableShardImpliesMutableIndex() {
        final IndexLongFieldRange range = randomSpecificRange(false);
        assertThat(range.extendWithShardRange(
                IntStream.of(range.getShards()).max().orElse(0) + 1,
                between(1, 10),
                ShardLongFieldRange.MUTABLE),
                sameInstance(IndexLongFieldRange.MUTABLE));
    }

    public void testExtendWithKnownShardIsNoOp() {
        IndexLongFieldRange range = randomSpecificRange();
        if (range == IndexLongFieldRange.UNKNOWN) {
            // need at least one known shard
            range = range.extendWithShardRange(between(0, 5), 5, ShardLongFieldRange.EMPTY);
        }

        final ShardLongFieldRange shardRange;
        if (range.getMinUnsafe() == IndexLongFieldRange.EMPTY.getMinUnsafe()
                && range.getMaxUnsafe() == IndexLongFieldRange.EMPTY.getMaxUnsafe()) {
            shardRange = ShardLongFieldRange.EMPTY;
        } else {
            final long min = randomLongBetween(range.getMinUnsafe(), range.getMaxUnsafe());
            final long max = randomLongBetween(min, range.getMaxUnsafe());
            shardRange = randomBoolean() ? ShardLongFieldRange.EMPTY : ShardLongFieldRange.of(min, max);
        }

        assertThat(range.extendWithShardRange(
                range.isComplete() ? between(1, 10) : randomFrom(IntStream.of(range.getShards()).boxed().collect(Collectors.toList())),
                between(1, 10),
                shardRange),
                sameInstance(range));
    }

    public void testExtendMutableRangeIsNoOp() {
        assertThat(IndexLongFieldRange.MUTABLE.extendWithShardRange(
                between(0, 10),
                between(0, 10),
                ShardLongFieldRangeWireTests.randomRange()),
                sameInstance(IndexLongFieldRange.MUTABLE));
    }

    public void testCompleteEmptyRangeIsEmptyInstance() {
        final int shardCount = between(1, 5);
        IndexLongFieldRange range = IndexLongFieldRange.UNKNOWN;
        for (int i = 0; i < shardCount; i++) {
            assertFalse(range.isComplete());
            range = range.extendWithShardRange(i, shardCount, ShardLongFieldRange.EMPTY);
        }
        assertThat(range, sameInstance(IndexLongFieldRange.EMPTY));
        assertTrue(range.isComplete());
    }

    public void testIsCompleteWhenAllShardRangesIncluded() {
        final int shardCount = between(1, 5);
        IndexLongFieldRange range = IndexLongFieldRange.UNKNOWN;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (int i = 0; i < shardCount; i++) {
            assertFalse(range.isComplete());
            final ShardLongFieldRange shardFieldRange;
            if (randomBoolean()) {
                shardFieldRange = ShardLongFieldRange.EMPTY;
            } else {
                shardFieldRange = ShardLongFieldRangeWireTests.randomSpecificRange();
                min = Math.min(min, shardFieldRange.getMin());
                max = Math.max(max, shardFieldRange.getMax());
            }
            range = range.extendWithShardRange(
                    i,
                    shardCount,
                    shardFieldRange);
        }
        assertTrue(range.isComplete());
        if (range != IndexLongFieldRange.EMPTY) {
            assertThat(range.getMin(), equalTo(min));
            assertThat(range.getMax(), equalTo(max));
        } else {
            assertThat(min, equalTo(Long.MAX_VALUE));
            assertThat(max, equalTo(Long.MIN_VALUE));
        }
    }

}
