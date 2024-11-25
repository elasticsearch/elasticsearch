/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CCSPartialResultsIT extends AbstractMultiClustersTestCase {

    protected Collection<Class<? extends Plugin>> nodePlugins(String cluster) {
        return Collections.singletonList(LocalStateEQLXPackPlugin.class);
    }

    protected final Client localClient() {
        return client(LOCAL_CLUSTER);
    }

    protected Collection<String> remoteClusterAlias() {
        return List.of("cluster-a", "cluster-b");
    }

    public void testFailuresFromRemote() throws ExecutionException, InterruptedException, IOException {
        final Client localClient = localClient();
        String REMOTE_CLUSTER = "cluster-b";
        final Client remoteClient = client(REMOTE_CLUSTER);

        assertAcked(
            remoteClient.admin()
                .indices()
                .prepareCreate("test-1-remote")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.routing.allocation.include._name", clusters().get(REMOTE_CLUSTER).getNodeNames()[0])
                        .build()
                )
                .setMapping("@timestamp", "type=date"),
            TimeValue.timeValueSeconds(60)
        );

        assertAcked(
            remoteClient.admin()
                .indices()
                .prepareCreate("test-2-remote")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.routing.allocation.exclude._name", clusters().get(REMOTE_CLUSTER).getNodeNames()[0])
                        .build()
                )
                .setMapping("@timestamp", "type=date"),
            TimeValue.timeValueSeconds(60)
        );

        for (int i = 0; i < 5; i++) {
            int val = i * 2;
            remoteClient.prepareIndex("test-1-remote")
                .setId(Integer.toString(i))
                .setSource("@timestamp", 100000 + val, "event.category", "process", "key", "same", "value", val)
                .get();
        }
        for (int i = 0; i < 5; i++) {
            int val = i * 2 + 1;
            remoteClient.prepareIndex("test-2-remote")
                .setId(Integer.toString(i))
                .setSource("@timestamp", 100000 + val, "event.category", "process", "key", "same", "value", val)
                .get();
        }

        remoteClient.admin().indices().prepareRefresh().get();
        localClient.admin().indices().prepareRefresh().get();

        // ------------------------------------------------------------------------
        // queries with full cluster (no missing shards)
        // ------------------------------------------------------------------------

        // event query
        var request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("process where true")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        EqlSearchResponse response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().events().size(), equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(response.hits().events().get(i).toString(), containsString("\"value\" : " + i));
        }
        assertThat(response.shardFailures().length, is(0));

        // sequence query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 2]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        EqlSearchResponse.Sequence sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 2"));
        assertThat(response.shardFailures().length, is(0));

        // sequence query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 3]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 3"));
        assertThat(response.shardFailures().length, is(0));

        // sequence query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 0] [process where value == 2]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 0"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 2"));
        assertThat(response.shardFailures().length, is(0));

        // sequence query with missing event on unavailable shard
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence with maxspan=10s [process where value == 1] ![process where value == 2] [process where value == 3]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(0));

        // sample query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 1]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        EqlSearchResponse.Sequence sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 2"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(0));

        // sample query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 3] [process where value == 1]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 3"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(0));

        // sample query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 0]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 2"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 0"));
        assertThat(response.shardFailures().length, is(0));

        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        cluster(REMOTE_CLUSTER).stopNode(clusters().get(REMOTE_CLUSTER).getNodeNames()[0]);

        // ------------------------------------------------------------------------
        // same queries, with missing shards and allow_partial_search_results=true
        // and allow_partial_sequence_result=true
        // ------------------------------------------------------------------------

        // event query
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*").query("process where true").allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().events().size(), equalTo(5));
        for (int i = 0; i < 5; i++) {
            assertThat(response.hits().events().get(i).toString(), containsString("\"value\" : " + (i * 2 + 1)));
        }
        assertThat(response.shardFailures().length, is(1));

        // sequence query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 2]")
            .allowPartialSearchResults(true)
            .allowPartialSequenceResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 3]")
            .allowPartialSearchResults(true)
            .allowPartialSequenceResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 3"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 0] [process where value == 2]")
            .allowPartialSearchResults(true)
            .allowPartialSequenceResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query with missing event on unavailable shard. THIS IS A FALSE POSITIVE
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence with maxspan=10s  [process where value == 1] ![process where value == 2] [process where value == 3]")
            .allowPartialSearchResults(true)
            .allowPartialSequenceResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(2).toString(), containsString("\"value\" : 3"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 1]")
            .allowPartialSearchResults(true)
            .allowPartialSequenceResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 3] [process where value == 1]")
            .allowPartialSearchResults(true)
            .allowPartialSequenceResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 3"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 0]")
            .allowPartialSearchResults(true)
            .allowPartialSequenceResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // ------------------------------------------------------------------------
        // same queries, with missing shards and allow_partial_search_results=true
        // and default allow_partial_sequence_results (ie. false)
        // ------------------------------------------------------------------------

        // event query
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*").query("process where true").allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().events().size(), equalTo(5));
        for (int i = 0; i < 5; i++) {
            assertThat(response.hits().events().get(i).toString(), containsString("\"value\" : " + (i * 2 + 1)));
        }
        assertThat(response.shardFailures().length, is(1));

        // sequence query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 2]")
            .allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 3]")
            .allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 0] [process where value == 2]")
            .allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query with missing event on unavailable shard. THIS IS A FALSE POSITIVE
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence with maxspan=10s  [process where value == 1] ![process where value == 2] [process where value == 3]")
            .allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 1]")
            .allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 3] [process where value == 1]")
            .allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 3"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 0]")
            .allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // ------------------------------------------------------------------------
        // same queries, with missing shards and with default xpack.eql.default_allow_partial_results=true
        // ------------------------------------------------------------------------

        cluster(REMOTE_CLUSTER).client()
            .execute(
                ClusterUpdateSettingsAction.INSTANCE,
                new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                    Settings.builder().put(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey(), true)
                )
            )
            .get();

        // event query
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*").query("process where true");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().events().size(), equalTo(5));
        for (int i = 0; i < 5; i++) {
            assertThat(response.hits().events().get(i).toString(), containsString("\"value\" : " + (i * 2 + 1)));
        }
        assertThat(response.shardFailures().length, is(1));

        // sequence query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 2]");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 3]");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 0] [process where value == 2]");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query with missing event on unavailable shard. THIS IS A FALSE POSITIVE
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence with maxspan=10s  [process where value == 1] ![process where value == 2] [process where value == 3]");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 1]");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 3] [process where value == 1]");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 3"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 0]");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        localClient().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                Settings.builder().putNull(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey())
            )
        ).get();
    }
}
