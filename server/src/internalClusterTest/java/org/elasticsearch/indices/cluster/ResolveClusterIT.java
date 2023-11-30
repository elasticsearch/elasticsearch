/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.resolve.ResolveClusterAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class ResolveClusterIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER_1 = "remote1";
    private static final String REMOTE_CLUSTER_2 = "remote2";
    private static long EARLIEST_TIMESTAMP = 1691348810000L;
    private static long LATEST_TIMESTAMP = 1691348820000L;

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, randomBoolean(), REMOTE_CLUSTER_2, true);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    public void testClusterResolveWithIndices() {
        Map<String, Object> testClusterInfo = setupThreeClusters(false); // TODO: needs params for datastreams
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex1 = (String) testClusterInfo.get("remote1.index");
        String remoteIndex2 = (String) testClusterInfo.get("remote2.index");
        boolean skipUnavailable1 = (Boolean) testClusterInfo.get("remote1.skip_unavailable");
        boolean skipUnavailable2 = true;

        // all clusters and both have matching indices
        {
            String[] indexExpressions = new String[] {
                localIndex,
                REMOTE_CLUSTER_1 + ":" + remoteIndex1,
                REMOTE_CLUSTER_2 + ":" + remoteIndex2 };
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterAction.ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterAction.ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterAction.ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // only remote clusters have matching indices
        {
            String[] indexExpressions = new String[] { "f*", REMOTE_CLUSTER_1 + ":" + remoteIndex1, REMOTE_CLUSTER_2 + ":" + remoteIndex2 };
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterAction.ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterAction.ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterAction.ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(false));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // only local cluster has matching indices
        {
            String[] indexExpressions = new String[] {
                localIndex,
                REMOTE_CLUSTER_1 + ":" + localIndex,
                REMOTE_CLUSTER_2 + ":" + localIndex };
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterAction.ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(false));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterAction.ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(false));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterAction.ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // test with wildcard expressions in the index - all clusters should match
        {
            String[] indexExpressions = new String[] {
                localIndex.substring(0, 2) + "*",
                REMOTE_CLUSTER_1 + ":" + remoteIndex1.substring(0, 4) + "*",
                REMOTE_CLUSTER_2 + ":*" };
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterAction.ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterAction.ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterAction.ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // test with wildcard expressions in the cluster and index - all clusters should match
        {
            String[] indexExpressions = new String[] {
                localIndex.substring(0, 2) + "*",
                REMOTE_CLUSTER_1.substring(0, 4) + "*:" + remoteIndex1.substring(0, 3) + "*" };
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterAction.ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterAction.ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(true));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterAction.ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // only remote1 included
        {
            String[] indexExpressions = new String[] { REMOTE_CLUSTER_1 + ":*" };
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(1, clusterInfo.size());
            assertThat(clusterInfo.keySet(), equalTo(Set.of(REMOTE_CLUSTER_1)));

            ResolveClusterAction.ResolveClusterInfo remote = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote.isConnected(), equalTo(true));
            assertThat(remote.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote.getMatchingIndices(), equalTo(true));
            assertNotNull(remote.getBuild().version());
        }

        // cluster exclusions
        {
            String[] indexExpressions = new String[] { "*", "rem*:*", "-remote1:*" };
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(2, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterAction.ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(true));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterAction.ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // index exclusions
        {
            String[] indexExpressions = new String[] { "*", "rem*:*", "-remote1:*", "-" + localIndex };
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(2, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterAction.ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(true));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterAction.ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(false));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }
    }

    public void testClusterResolveWithMatchingAliases() {
        Map<String, Object> testClusterInfo = setupThreeClusters(true); // TODO: needs params for datastreams
        String localAlias = (String) testClusterInfo.get("local.alias");
        String remoteAlias1 = (String) testClusterInfo.get("remote1.alias");
        String remoteAlias2 = (String) testClusterInfo.get("remote2.alias");
        boolean skipUnavailable1 = (Boolean) testClusterInfo.get("remote1.skip_unavailable");
        boolean skipUnavailable2 = true;

        // all clusters and both have matching indices
        {
            String[] indexExpressions = new String[] {
                localAlias,
                REMOTE_CLUSTER_1 + ":" + remoteAlias1,
                REMOTE_CLUSTER_2 + ":" + remoteAlias2 };
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterAction.ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterAction.ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterAction.ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // only remote cluster has matching indices
        {
            String[] indexExpressions = new String[] { "f*", REMOTE_CLUSTER_1 + ":" + remoteAlias1, REMOTE_CLUSTER_2 + ":" + remoteAlias2 };
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterAction.ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());

            ResolveClusterAction.ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());

            ResolveClusterAction.ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(false));
            assertNotNull(local.getBuild().version());
        }

        // only local cluster has matching alias and cluster exclusion
        {
            String[] indexExpressions = new String[] { localAlias, "*:foo", "-" + REMOTE_CLUSTER_2 + ":*" };
            ResolveClusterAction.Request request = new ResolveClusterAction.Request(indexExpressions);

            ActionFuture<ResolveClusterAction.Response> future = client(LOCAL_CLUSTER).admin().indices().resolveCluster(request);
            ResolveClusterAction.Response response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterAction.ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(2, clusterInfo.size());
            assertThat(clusterInfo.keySet(), equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1)));

            ResolveClusterAction.ResolveClusterInfo remote = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote.isConnected(), equalTo(true));
            assertThat(remote.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote.getMatchingIndices(), equalTo(false));
            assertNotNull(remote.getBuild().version());

            ResolveClusterAction.ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
        }

    }

    private Map<String, Object> setupThreeClusters(boolean useAlias) {
        String localAlias = randomAlphaOfLengthBetween(5, 25);
        String remoteAlias1 = randomAlphaOfLengthBetween(5, 25);
        String remoteAlias2 = randomAlphaOfLengthBetween(5, 25);

        // set up local cluster and index

        String localIndex = "demo";
        int numShardsLocal = randomIntBetween(3, 6);
        Settings localSettings = indexSettings(numShardsLocal, 0).build();

        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .indices()
                .prepareCreate(localIndex)
                .setSettings(localSettings)
                .setMapping("@timestamp", "type=date", "f", "type=text")
        );
        if (useAlias) {
            // local index alias
            IndicesAliasesRequest.AliasActions addAction = new IndicesAliasesRequest.AliasActions(
                IndicesAliasesRequest.AliasActions.Type.ADD
            ).index(localIndex).aliases(localAlias);
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
            aliasesAddRequest.addAliasAction(addAction);

            assertAcked(client(LOCAL_CLUSTER).admin().indices().aliases(aliasesAddRequest));
        }
        indexDocs(client(LOCAL_CLUSTER), localIndex);

        // set up remote1 cluster and index

        String remoteIndex1 = "prod";
        int numShardsRemote1 = randomIntBetween(3, 6);
        final InternalTestCluster remoteCluster1 = cluster(REMOTE_CLUSTER_1);
        remoteCluster1.ensureAtLeastNumDataNodes(randomIntBetween(1, 3));
        final Settings.Builder remoteSettings1 = Settings.builder();
        remoteSettings1.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShardsRemote1);

        assertAcked(
            client(REMOTE_CLUSTER_1).admin()
                .indices()
                .prepareCreate(remoteIndex1)
                .setSettings(Settings.builder().put(remoteSettings1.build()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                .setMapping("@timestamp", "type=date", "f", "type=text")
        );
        if (useAlias) {
            // remote1 index alias
            IndicesAliasesRequest.AliasActions addAction = new IndicesAliasesRequest.AliasActions(
                IndicesAliasesRequest.AliasActions.Type.ADD
            ).index(remoteIndex1).aliases(remoteAlias1);
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
            aliasesAddRequest.addAliasAction(addAction);

            assertAcked(client(REMOTE_CLUSTER_1).admin().indices().aliases(aliasesAddRequest));
        }

        assertFalse(
            client(REMOTE_CLUSTER_1).admin()
                .cluster()
                .prepareHealth(remoteIndex1)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        indexDocs(client(REMOTE_CLUSTER_1), remoteIndex1);

        // set up remote2 cluster and index

        String remoteIndex2 = "prod123";
        int numShardsRemote2 = randomIntBetween(2, 4);
        final InternalTestCluster remoteCluster2 = cluster(REMOTE_CLUSTER_2);
        remoteCluster2.ensureAtLeastNumDataNodes(randomIntBetween(1, 2));
        final Settings.Builder remoteSettings2 = Settings.builder();
        remoteSettings2.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShardsRemote2);

        assertAcked(
            client(REMOTE_CLUSTER_2).admin()
                .indices()
                .prepareCreate(remoteIndex2)
                .setSettings(Settings.builder().put(remoteSettings2.build()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                .setMapping("@timestamp", "type=date", "f", "type=text")
        );
        if (useAlias) {
            // remote2 index alias
            IndicesAliasesRequest.AliasActions addAction = new IndicesAliasesRequest.AliasActions(
                IndicesAliasesRequest.AliasActions.Type.ADD
            ).index(remoteIndex2).aliases(remoteAlias2);
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
            aliasesAddRequest.addAliasAction(addAction);

            assertAcked(client(REMOTE_CLUSTER_2).admin().indices().aliases(aliasesAddRequest));
        }

        assertFalse(
            client(REMOTE_CLUSTER_2).admin()
                .cluster()
                .prepareHealth(remoteIndex2)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        indexDocs(client(REMOTE_CLUSTER_2), remoteIndex2);

        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER_1);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER_1).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable1 = (boolean) cluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).clusterService()
            .getClusterSettings()
            .get(skipUnavailableSetting);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.index", localIndex);

        clusterInfo.put("remote1.index", remoteIndex1);
        clusterInfo.put("remote1.skip_unavailable", skipUnavailable1);

        clusterInfo.put("remote2.index", remoteIndex2);
        clusterInfo.put("remote2.skip_unavailable", true);

        if (useAlias) {
            clusterInfo.put("local.alias", localAlias);
            clusterInfo.put("remote1.alias", remoteAlias1);
            clusterInfo.put("remote2.alias", remoteAlias2);
        }
        return clusterInfo;
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(50, 100);
        for (int i = 0; i < numDocs; i++) {
            long ts = EARLIEST_TIMESTAMP + i;
            if (i == numDocs - 1) {
                ts = LATEST_TIMESTAMP;
            }
            client.prepareIndex(index).setSource("f", "v", "@timestamp", ts).get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }
}
