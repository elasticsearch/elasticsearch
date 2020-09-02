/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.ilm.history.ILMHistoryStore;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.ilm.LifecyclePolicyTestsUtils.randomTimeseriesLifecyclePolicy;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ILMHistoryTests extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");
        settings.put(LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), false);
        return settings.build();
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class);
    }

    private void putTestPolicy() throws InterruptedException, java.util.concurrent.ExecutionException {
        LifecyclePolicy lifecyclePolicy = randomTimeseriesLifecyclePolicy("test");
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        PutLifecycleAction.Response putLifecycleResponse = client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get();
        assertAcked(putLifecycleResponse);
    }

    public void testIlmHistoryIndexCanRollover() throws Exception {
        putTestPolicy();
        Settings settings = Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0).put(LifecycleSettings.LIFECYCLE_NAME, "test").build();
        CreateIndexResponse res = client().admin().indices().prepareCreate("test").setSettings(settings).get();
        assertTrue(res.isAcknowledged());

        assertBusy(() -> {
            String firstIndex = ILMHistoryStore.ILM_HISTORY_INDEX_PREFIX + "000001";
            try {
                GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().setIndices(firstIndex).get();
                assertThat(getIndexResponse.getIndices(), arrayContaining(firstIndex));
            } catch (Exception e) {
                fail(e.getMessage());
            }
        });

        RolloverResponse rolloverResponse = client().admin().indices().prepareRolloverIndex(ILMHistoryStore.ILM_HISTORY_ALIAS).get();
        String secondIndex = ILMHistoryStore.ILM_HISTORY_INDEX_PREFIX + "000002";
        assertTrue(rolloverResponse.isAcknowledged());
        assertThat(rolloverResponse.getNewIndex(), is(secondIndex));

        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().setIndices(secondIndex).get();
        assertThat(getIndexResponse.getIndices(), arrayContaining(secondIndex));

        //wait for all history items to index to avoid waiting for timeout in ILMHistoryStore beforeBulk
        Thread.sleep(6000);
    }
}
