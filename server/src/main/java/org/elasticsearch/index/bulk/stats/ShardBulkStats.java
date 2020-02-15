/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.bulk.stats;

import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.index.shard.IndexShard;

import java.util.concurrent.TimeUnit;

/**
 * Internal class that maintains relevant shard bulk statistics / metrics.
 * @see IndexShard
 */
public class ShardBulkStats implements BulkOperationListener {

    private final StatsHolder totalStats = new StatsHolder();

    public BulkStats stats() {
        return totalStats.stats();
    }

    @Override
    public void afterBulk(long shardBulkSizeInBytes, long tookInNanos) {
        totalStats.totalSizeInBytes.inc(shardBulkSizeInBytes);
        totalStats.shardBulkMetric.inc(tookInNanos);
    }

    static final class StatsHolder {
        final MeanMetric shardBulkMetric = new MeanMetric();
        final CounterMetric totalSizeInBytes = new CounterMetric();

        BulkStats stats() {
            return new BulkStats(shardBulkMetric.count(), TimeUnit.NANOSECONDS.toMillis(shardBulkMetric.sum()), totalSizeInBytes.count());
        }
    }
}
