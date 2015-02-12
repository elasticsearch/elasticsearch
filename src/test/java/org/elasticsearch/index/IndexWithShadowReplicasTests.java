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

package org.elasticsearch.index;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests for indices that use shadow replicas and a shared filesystem
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
@TestLogging("_root:DEBUG,env:TRACE") //nocommit
public class IndexWithShadowReplicasTests extends ElasticsearchIntegrationTest {

    @Test
    public void testIndexWithFewDocuments() throws Exception {
        Settings nodeSettings = ImmutableSettings.builder()
                .put("node.add_id_to_custom_path", false)
                .put("node.enable_custom_paths", true)
                .build();

        internalCluster().startNodesAsync(3, nodeSettings).get();
        final String IDX = "test";
        final Path dataPath = newTempDirPath();

        Settings idxSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).get();
        ensureGreen(IDX);

        // So basically, the primary should fail and the replica will need to
        // replay the translog, this is what this tests
        client().prepareIndex(IDX, "doc", "1").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();
        flushAndRefresh(IDX);
        client().prepareIndex(IDX, "doc", "3").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "4").setSource("foo", "bar").get();
        refresh();

        // Check that we can get doc 1 and 2
        GetResponse gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        GetResponse gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        logger.info("--> restarting both nodes");
        if (randomBoolean()) {
            logger.info("--> rolling restart");
            internalCluster().rollingRestart();
        } else {
            logger.info("--> full restart");
            internalCluster().fullRestart();
        }

        client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
        ensureGreen(IDX);

        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, 4);

        logger.info("--> deleting index");
        assertAcked(client().admin().indices().prepareDelete(IDX));
    }

    @Test
    public void testReplicaToPrimaryPromotion() throws Exception {
        Settings nodeSettings = ImmutableSettings.builder()
                .put("node.add_id_to_custom_path", false)
                .put("node.enable_custom_paths", true)
                .build();

        String node1 = internalCluster().startNode(nodeSettings);
        Path dataPath = newTempDirPath();
        String IDX = "test";

        Settings idxSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=string").get();
        ensureYellow(IDX);
        client().prepareIndex(IDX, "doc", "1").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();

        GetResponse gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        GetResponse gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertTrue(gResp1.isExists());
        assertTrue(gResp2.isExists());
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        // Node1 has the primary, now node2 has the replica
        String node2 = internalCluster().startNode(nodeSettings);
        ensureGreen(IDX);
        client().admin().cluster().prepareHealth().setWaitForNodes("2").get();
        flushAndRefresh(IDX);

        logger.info("--> stopping node1 [{}]", node1);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node1));
        ensureYellow(IDX);

        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, 2);

        gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertTrue(gResp1.isExists());
        assertTrue(gResp2.toString(), gResp2.isExists());
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));
    }

    @Test
    public void testIndexWithShadowReplicasCleansUp() throws Exception {
        Settings nodeSettings = ImmutableSettings.builder()
                .put("node.add_id_to_custom_path", false)
                .put("node.enable_custom_paths", true)
                .build();

        int nodeCount = randomIntBetween(2, 5);
        internalCluster().startNodesAsync(nodeCount, nodeSettings).get();
        Path dataPath = newTempDirPath();
        String IDX = "test";

        Settings idxSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(1, nodeCount - 1))
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).get();
        ensureGreen(IDX);
        client().prepareIndex(IDX, "doc", "1").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();
        flushAndRefresh(IDX);

        GetResponse gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        GetResponse gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, 2);

        assertAcked(client().admin().indices().prepareDelete(IDX));

        assertPathHasBeenCleared(dataPath);
    }

    /**
     * Tests that shadow replicas can be "naturally" rebalanced and relocated
     * around the cluster. By "naturally" I mean without using the reroute API
     * @throws Exception
     */
    @Test
    public void testShadowReplicaNaturalRelocation() throws Exception {
        Settings nodeSettings = ImmutableSettings.builder()
                .put("node.add_id_to_custom_path", false)
                .put("node.enable_custom_paths", true)
                .build();

        internalCluster().startNodesAsync(2, nodeSettings).get();
        Path dataPath = newTempDirPath();
        String IDX = "test";

        Settings idxSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 10)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).get();
        ensureGreen(IDX);

        int docCount = randomIntBetween(10, 100);
        List<IndexRequestBuilder> builders = newArrayList();
        for (int i = 0; i < docCount; i++) {
            builders.add(client().prepareIndex(IDX, "doc", i + "").setSource("body", "foo"));
        }
        indexRandom(true, true, true, builders);
        flushAndRefresh(IDX);

        // start a third node, with 10 shards each on the other nodes, they
        // should relocate some to the third node
        final String node3 = internalCluster().startNode(nodeSettings);

        assertBusy(new Runnable() {
            @Override
            public void run() {
                client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
                ClusterStateResponse resp = client().admin().cluster().prepareState().get();
                RoutingNodes nodes = resp.getState().getRoutingNodes();
                for (RoutingNode node : nodes) {
                    logger.info("--> node has {} shards", node.numberOfOwningShards());
                    assertThat("at least 5 shards on node", node.numberOfOwningShards(), greaterThanOrEqualTo(5));
                }
            }
        });
        ensureGreen(IDX);

        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, docCount);

        assertAcked(client().admin().indices().prepareDelete(IDX));

        assertPathHasBeenCleared(dataPath);
    }
}
