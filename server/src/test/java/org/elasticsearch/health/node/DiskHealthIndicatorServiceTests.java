/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Ignore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DiskHealthIndicatorServiceTests extends ESTestCase {
    public void testServiceBasics() {
        Set<DiscoveryNode> discoveryNodes = createNodes();
        ClusterService clusterService = createClusterService(false, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        {
            HealthStatus expectedStatus = HealthStatus.GREEN;
            HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(expectedStatus));
        }
        {
            HealthStatus expectedStatus = HealthStatus.YELLOW;
            HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(expectedStatus));
        }
        {
            HealthStatus expectedStatus = HealthStatus.RED;
            HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(expectedStatus));
        }
    }

    @SuppressWarnings("unchecked")
    public void testGreen() throws IOException {
        Set<DiscoveryNode> discoveryNodes = createNodes();
        ClusterService clusterService = createClusterService(false, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = HealthStatus.GREEN;
        HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(result.symptom(), equalTo("The cluster has enough available disk space."));
        assertThat(result.impacts().size(), equalTo(0));
        assertThat(result.diagnosisList().size(), equalTo(0));
        Map<String, Object> detailsMap = xContentToMap(result.details());
        assertThat(detailsMap.size(), equalTo(1));
        List<Map<String, String>> nodeDetails = (List<Map<String, String>>) detailsMap.get("nodes");
        assertThat(nodeDetails.size(), equalTo(discoveryNodes.size()));
        Map<String, String> nodeIdToName = discoveryNodes.stream().collect(Collectors.toMap(DiscoveryNode::getId, DiscoveryNode::getName));
        for (Map<String, String> nodeDetail : nodeDetails) {
            assertThat(nodeDetail.size(), greaterThanOrEqualTo(3));
            assertThat(nodeDetail.size(), lessThanOrEqualTo(4)); // Could have a cause
            String nodeId = nodeDetail.get("node_id");
            assertThat(nodeDetail.get("name"), equalTo(nodeIdToName.get(nodeId)));
            assertThat(nodeDetail.get("status"), equalTo("GREEN"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testRedNoBlocksNoIndices() throws IOException {
        Set<DiscoveryNode> discoveryNodes = createNodes();
        ClusterService clusterService = createClusterService(false, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = HealthStatus.RED;
        HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(result.symptom(), equalTo("1 data node is out of disk space."));
        assertThat(result.impacts().size(), equalTo(1));
        HealthIndicatorImpact impact = result.impacts().get(0);
        assertNotNull(impact);
        List<ImpactArea> impactAreas = impact.impactAreas();
        assertThat(impactAreas.size(), equalTo(1));
        assertThat(impactAreas.get(0), equalTo(ImpactArea.INGEST));
        assertThat(impact.severity(), equalTo(1));
        assertThat(impact.impactDescription(), equalTo("Cannot insert or update documents in the affected indices."));
        assertThat(result.diagnosisList().size(), equalTo(1));
        Diagnosis diagnosis = result.diagnosisList().get(0);
        List<String> affectedResources = diagnosis.affectedResources();
        assertThat(affectedResources.size(), equalTo(1));
        String expectedRedNodeId = healthInfo.diskInfoByNode()
            .entrySet()
            .stream()
            .filter(entry -> expectedStatus.equals(entry.getValue().healthStatus()))
            .map(Map.Entry::getKey)
            .findAny()
            .orElseThrow();
        assertThat(affectedResources.get(0), equalTo(expectedRedNodeId));
        Map<String, Object> detailsMap = xContentToMap(result.details());
        assertThat(detailsMap.size(), equalTo(1));
        List<Map<String, String>> nodeDetails = (List<Map<String, String>>) detailsMap.get("nodes");
        assertThat(nodeDetails.size(), equalTo(discoveryNodes.size()));
        Map<String, String> nodeIdToName = discoveryNodes.stream().collect(Collectors.toMap(DiscoveryNode::getId, DiscoveryNode::getName));
        for (Map<String, String> nodeDetail : nodeDetails) {
            assertThat(nodeDetail.size(), greaterThanOrEqualTo(3));
            assertThat(nodeDetail.size(), lessThanOrEqualTo(4)); // Could have a cause
            String nodeId = nodeDetail.get("node_id");
            assertThat(nodeDetail.get("name"), equalTo(nodeIdToName.get(nodeId)));
            if (nodeId.equals(expectedRedNodeId)) {
                assertThat(nodeDetail.get("status"), equalTo("RED"));
            } else {
                assertThat(nodeDetail.get("status"), equalTo("GREEN"));
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Ignore
    public void testRedNoBlocksWithIndices() throws IOException {
        Set<DiscoveryNode> discoveryNodes = createNodes();
        ClusterService clusterService = createClusterService(false, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = HealthStatus.RED;
        HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(result.symptom(), equalTo("3 indices are blocked and cannot be updated because 2 nodes are out of space."));
        assertThat(result.impacts().size(), equalTo(1));
        HealthIndicatorImpact impact = result.impacts().get(0);
        assertNotNull(impact);
        List<ImpactArea> impactAreas = impact.impactAreas();
        assertThat(impactAreas.size(), equalTo(1));
        assertThat(impactAreas.get(0), equalTo(ImpactArea.INGEST));
        assertThat(impact.severity(), equalTo(1));
        assertThat(impact.impactDescription(), equalTo("Cannot insert or update documents in the affected indices."));
        assertThat(result.diagnosisList().size(), equalTo(1));
        Diagnosis diagnosis = result.diagnosisList().get(0);
        List<String> affectedResources = diagnosis.affectedResources();
        assertThat(affectedResources.size(), equalTo(1));
        String expectedRedNodeId = healthInfo.diskInfoByNode()
            .entrySet()
            .stream()
            .filter(entry -> expectedStatus.equals(entry.getValue().healthStatus()))
            .map(Map.Entry::getKey)
            .findAny()
            .orElseThrow();
        assertThat(affectedResources.get(0), equalTo(expectedRedNodeId));
        Map<String, Object> detailsMap = xContentToMap(result.details());
        assertThat(detailsMap.size(), equalTo(1));
        List<Map<String, String>> nodeDetails = (List<Map<String, String>>) detailsMap.get("nodes");
        assertThat(nodeDetails.size(), equalTo(discoveryNodes.size()));
        Map<String, String> nodeIdToName = discoveryNodes.stream().collect(Collectors.toMap(DiscoveryNode::getId, DiscoveryNode::getName));
        for (Map<String, String> nodeDetail : nodeDetails) {
            assertThat(nodeDetail.size(), greaterThanOrEqualTo(3));
            assertThat(nodeDetail.size(), lessThanOrEqualTo(4)); // Could have a cause
            String nodeId = nodeDetail.get("node_id");
            assertThat(nodeDetail.get("name"), equalTo(nodeIdToName.get(nodeId)));
            if (nodeId.equals(expectedRedNodeId)) {
                assertThat(nodeDetail.get("status"), equalTo("RED"));
            } else {
                assertThat(nodeDetail.get("status"), equalTo("GREEN"));
            }
        }
    }

    public void testBlockOtherwiseGreen() {
        /*
         * Tests when there is an index that has a block on it but the nodes report green (so the lock is probably about to be released).
         */
        Set<DiscoveryNode> discoveryNodes = createNodes();
        ClusterService clusterService = createClusterService(true, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        {
            HealthStatus expectedStatus = HealthStatus.RED;
            HealthInfo healthInfo = createHealthInfo(HealthStatus.GREEN, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(expectedStatus));
            assertThat(result.symptom(), equalTo("1 index is blocked and cannot be updated but 0 nodes are currently out of space."));
        }
    }

    public void testBlockOtherwiseYellow() {
        /*
         * Tests when there is an index that has a block on it but the nodes report yellow.
         */
        Set<DiscoveryNode> discoveryNodes = createNodes();
        ClusterService clusterService = createClusterService(true, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = HealthStatus.RED;
        int numberOfYellowNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(HealthStatus.YELLOW, numberOfYellowNodes, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(
            result.symptom(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "1 index is blocked and cannot be updated because %s running low on disk space.",
                    numberOfYellowNodes == 1 ? "1 node is" : numberOfYellowNodes + " nodes are"
                )
            )
        );
    }

    public void testBlockOtherwiseRed() {
        Set<DiscoveryNode> discoveryNodes = createNodes();
        int numberOfIndices = randomIntBetween(1, 20);
        int numberOfBlockedIndices = randomIntBetween(1, numberOfIndices);
        ClusterService clusterService = createClusterService(numberOfIndices, numberOfBlockedIndices, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = HealthStatus.RED;
        int numberOfRedNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(HealthStatus.RED, numberOfRedNodes, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(
            result.symptom(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "%s blocked and cannot be updated because %s out of disk space.",
                    numberOfBlockedIndices == 1 ? "1 index is" : numberOfBlockedIndices + " indices are",
                    numberOfRedNodes == 1 ? "1 node is" : numberOfRedNodes + " nodes are"
                )
            )
        );
    }

    public void testMissingHealthInfo() {
        Set<DiscoveryNode> discoveryNodes = createNodes();
        Set<DiscoveryNode> discoveryNodesInClusterState = new HashSet<>(discoveryNodes);
        discoveryNodesInClusterState.add(
            new DiscoveryNode(
                randomAlphaOfLength(30),
                UUID.randomUUID().toString(),
                buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                DiscoveryNodeRole.roles(),
                Version.CURRENT
            )
        );
        ClusterService clusterService = createClusterService(false, discoveryNodesInClusterState);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        {
            HealthInfo healthInfo = HealthInfo.EMPTY_HEALTH_INFO;
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.UNKNOWN));
        }
        {
            HealthInfo healthInfo = createHealthInfo(HealthStatus.GREEN, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.GREEN));
        }
        {
            HealthInfo healthInfo = createHealthInfo(HealthStatus.YELLOW, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        }
        {
            HealthInfo healthInfo = createHealthInfo(HealthStatus.RED, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.RED));
        }
    }

    private Set<DiscoveryNode> createNodes() {
        int numberOfNodes = randomIntBetween(1, 200);
        Set<DiscoveryNode> discoveryNodes = new HashSet<>();
        for (int i = 0; i < numberOfNodes; i++) {
            discoveryNodes.add(
                new DiscoveryNode(
                    randomAlphaOfLength(30),
                    UUID.randomUUID().toString(),
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    DiscoveryNodeRole.roles(),
                    Version.CURRENT
                )
            );
        }
        return discoveryNodes;
    }

    private HealthInfo createHealthInfo(HealthStatus expectedStatus, Set<DiscoveryNode> nodes) {
        return createHealthInfo(expectedStatus, 1, nodes);
    }

    private HealthInfo createHealthInfo(HealthStatus expectedStatus, int numberOfNodesWithExpectedStatus, Set<DiscoveryNode> nodes) {
        assert numberOfNodesWithExpectedStatus <= nodes.size();
        Map<String, DiskHealthInfo> diskInfoByNode = new HashMap<>(nodes.size());
        int numberWithNonGreenStatus = 0;
        for (DiscoveryNode node : nodes) {
            final DiskHealthInfo diskHealthInfo;
            if (numberWithNonGreenStatus < numberOfNodesWithExpectedStatus) {
                diskHealthInfo = randomBoolean()
                    ? new DiskHealthInfo(expectedStatus)
                    : new DiskHealthInfo(expectedStatus, randomFrom(DiskHealthInfo.Cause.values()));
                numberWithNonGreenStatus++;
            } else {
                diskHealthInfo = randomBoolean()
                    ? new DiskHealthInfo(HealthStatus.GREEN)
                    : new DiskHealthInfo(HealthStatus.GREEN, randomFrom(DiskHealthInfo.Cause.values()));
            }
            diskInfoByNode.put(node.getId(), diskHealthInfo);
        }
        return new HealthInfo(diskInfoByNode);
    }

    private static ClusterService createClusterService(boolean blockIndex, Set<DiscoveryNode> nodes) {
        return createClusterService(1, blockIndex ? 1 : 0, nodes);
    }

    private static ClusterService createClusterService(int numberOfIndices, int numberOfIndicesToBlock, Set<DiscoveryNode> nodes) {
        var routingTableBuilder = RoutingTable.builder();
        ClusterBlocks.Builder clusterBlocksBuilder = new ClusterBlocks.Builder();
        int blockedIndices = 0;
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        List<ClusterBlocks> clusterBlocksList = new ArrayList<>();
        for (int i = 0; i < numberOfIndices; i++) {
            boolean blockIndex = blockedIndices < numberOfIndicesToBlock;
            blockedIndices++;
            IndexMetadata indexMetadata = new IndexMetadata.Builder(randomAlphaOfLength(20)).settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), blockIndex)
                    .build()
            ).build();
            indexMetadataMap.put(indexMetadata.getIndex().getName(), indexMetadata);
            if (blockIndex) {
                ClusterBlocks clusterBlocks = clusterBlocksBuilder.addBlocks(indexMetadata).build();
                clusterBlocksList.add(clusterBlocks);
            }
        }
        Metadata.Builder metadataBuilder = Metadata.builder();
        metadataBuilder.indices(indexMetadataMap);
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        for (DiscoveryNode node : nodes) {
            nodesBuilder.add(node);
        }
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(routingTableBuilder.build())
            .metadata(metadataBuilder.build())
            .nodes(nodesBuilder);
        for (ClusterBlocks clusterBlocks : clusterBlocksList) {
            clusterStateBuilder.blocks(clusterBlocks);
        }
        clusterStateBuilder.nodes(nodesBuilder);
        ClusterState clusterState = clusterStateBuilder.build();
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return clusterService;
    }

    private Map<String, Object> xContentToMap(ToXContent xcontent) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
        return parser.map();
    }
}
