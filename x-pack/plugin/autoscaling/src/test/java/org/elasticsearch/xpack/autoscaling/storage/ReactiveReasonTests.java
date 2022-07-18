/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.greaterThan;

public class ReactiveReasonTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testXContent() throws IOException {
        String reason = randomAlphaOfLength(10);
        long unassigned = randomNonNegativeLong();
        long assigned = randomNonNegativeLong();
        String indexUUID = UUIDs.randomBase64UUID();
        String indexName = randomAlphaOfLength(10);
        SortedSet<ShardId> unassignedShardIds = new TreeSet<>(randomUnique(() -> new ShardId(indexName, indexUUID, randomInt(1000)), 600));
        SortedSet<ShardId> assignedShardIds = new TreeSet<>(randomUnique(() -> new ShardId(indexName, indexUUID, randomInt(1000)), 600));
        AllocateUnassignedDecision unassignedDecision = AllocateUnassignedDecision.no(
            randomFrom(
                UnassignedInfo.AllocationStatus.DECIDERS_NO,
                UnassignedInfo.AllocationStatus.DELAYED_ALLOCATION,
                UnassignedInfo.AllocationStatus.NO_VALID_SHARD_COPY,
                UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA
            ),
            randomBoolean()
                ? List.of(
                    new NodeAllocationResult(
                        new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                        Decision.NO,
                        1
                    )
                )
                : List.of(),
            randomBoolean()
        );
        var reactiveReason = new ReactiveStorageDeciderService.ReactiveReason(
            reason,
            unassigned,
            unassignedShardIds,
            assigned,
            assignedShardIds,
            unassignedDecision
        );

        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                BytesReference.bytes(reactiveReason.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            )
        ) {
            Map<String, Object> map = parser.map();
            assertEquals(reason, map.get("reason"));
            assertEquals(unassigned, map.get("unassigned"));
            assertEquals(assigned, map.get("assigned"));

            List<String> xContentUnassignedShardIds = (List<String>) map.get("unassigned_shards");
            assertEquals(
                unassignedShardIds.stream()
                    .map(ShardId::toString)
                    .limit(ReactiveStorageDeciderService.ReactiveReason.MAX_AMOUNT_OF_SHARDS)
                    .toList(),
                xContentUnassignedShardIds
            );
            assertSorted(xContentUnassignedShardIds.stream().map(ShardId::fromString).toList());
            assertEquals(unassignedShardIds.size(), map.get("unassigned_shards_count"));

            List<String> xContentAssignedShardIds = (List<String>) map.get("assigned_shards");
            assertEquals(
                assignedShardIds.stream()
                    .map(ShardId::toString)
                    .limit(ReactiveStorageDeciderService.ReactiveReason.MAX_AMOUNT_OF_SHARDS)
                    .collect(Collectors.toList()),
                xContentAssignedShardIds
            );
            assertSorted(xContentAssignedShardIds.stream().map(ShardId::fromString).toList());
            assertEquals(assignedShardIds.size(), map.get("assigned_shards_count"));

            Map<String, Object> unassignedDecisionAsMap = (Map<String, Object>) map.get("unassigned_shard_allocate_decision");
            assertEquals(unassignedDecision.getAllocationDecision().toString(), unassignedDecisionAsMap.get("can_allocate"));
            assertEquals(unassignedDecision.getExplanation(), unassignedDecisionAsMap.get("allocate_explanation"));
            List<Object> nodeAllocationDecisions = (List<Object>) unassignedDecisionAsMap.get("node_allocation_decisions");
            if (unassignedDecision.getNodeDecisions().size() > 0) {
                Map<String, Object> nodeAllocationResult = (Map<String, Object>) nodeAllocationDecisions.get(0);
                assertEquals(
                    unassignedDecision.getNodeDecisions().get(0).getNodeDecision().toString(),
                    nodeAllocationResult.get("node_decision")
                );
                assertEquals(unassignedDecision.getNodeDecisions().get(0).getNode().getId(), nodeAllocationResult.get("node_id"));
                assertEquals(unassignedDecision.getNodeDecisions().get(0).getWeightRanking(), nodeAllocationResult.get("weight_ranking"));
            } else {
                assertNull(nodeAllocationDecisions);
            }
        }
    }

    private static void assertSorted(Collection<ShardId> collection) {
        ShardId previous = null;
        for (ShardId e : collection) {
            if (previous != null) {
                assertThat(e, greaterThan(previous));
            }
            previous = e;
        }
    }
}
