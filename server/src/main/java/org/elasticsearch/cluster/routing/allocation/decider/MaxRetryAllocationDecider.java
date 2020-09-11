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

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.Setting;

/**
 * An allocation decider that prevents shards from being allocated on any node if the shards allocation has been retried N times without
 * success. This means if a shard has been INITIALIZING N times in a row without being moved to STARTED the shard will be ignored until
 * the setting for {@code index.allocation.max_retry} is raised. The default value is {@code 5}.
 * Note: This allocation decider also allows allocation of repeatedly failing shards when the {@code /_cluster/reroute?retry_failed=true}
 * API is manually invoked. This allows single retries without raising the limits.
 *
 */
public class MaxRetryAllocationDecider extends AllocationDecider {

    public static final Setting<Integer> SETTING_ALLOCATION_MAX_RETRY = Setting.intSetting("index.allocation.max_retries", 5, 0,
        Setting.Property.Dynamic, Setting.Property.IndexScope, Setting.Property.NotCopyableOnResize);

    public static final String NAME = "max_retry";

    private static final Decision YES_NO_FAILURES = Decision.single(Decision.Type.YES, NAME, "shard has no previous failures");

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        final UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
        final Decision decision;
        final boolean debug = allocation.debugDecision();
        final int numFailedAllocations = unassignedInfo == null ? 0 : unassignedInfo.getNumFailedAllocations();
        if (numFailedAllocations > 0) {
            final IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
            final int maxRetry = SETTING_ALLOCATION_MAX_RETRY.get(indexMetadata.getSettings());
            final Decision res = numFailedAllocations >= maxRetry ? Decision.NO : Decision.YES;
            decision = debug ? debugDecision(res, unassignedInfo, numFailedAllocations, maxRetry) : res;
        } else {
            decision = debug ? YES_NO_FAILURES : Decision.YES;
        }
        return decision;
    }

    private static Decision debugDecision(Decision decision, UnassignedInfo unassignedInfo, int numFailedAllocations, int maxRetry) {
        if (decision.type() == Decision.Type.YES) {
            return Decision.single(Decision.Type.NO, NAME, "shard has exceeded the maximum number of retries [%d] on " +
                            "failed allocation attempts - manually call [/_cluster/reroute?retry_failed=true] to retry, [%s]",
                    maxRetry, unassignedInfo.toString());
        } else {
            return Decision.single(Decision.Type.YES, NAME, "shard has failed allocating [%d] times but [%d] retries are allowed",
                    numFailedAllocations, maxRetry);
        }
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, allocation);
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call canForceAllocatePrimary on a non-primary shard " + shardRouting;
        // check if we have passed the maximum retry threshold through canAllocate,
        // if so, we don't want to force the primary allocation here
        return canAllocate(shardRouting, node, allocation);
    }
}
