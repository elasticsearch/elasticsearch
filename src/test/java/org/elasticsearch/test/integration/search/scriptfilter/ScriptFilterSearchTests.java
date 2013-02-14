/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.search.scriptfilter;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.scriptFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@Test
public class ScriptFilterSearchTests extends AbstractNodesTests {

    private Client client;

    @BeforeMethod
    public void createNodes() throws Exception {
        Settings nodeSettings = ImmutableSettings.settingsBuilder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build();
        startNode("server1", nodeSettings);
        client = getClient();
    }

    @AfterMethod
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test
    public void testCustomScriptBoost() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception ex) {
            //
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 1.0f).endObject())
                .execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "type1", "2")
                .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 2.0f).endObject())
                .execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "type1", "3")
                .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 3.0f).endObject())
                .execute().actionGet();
        client.admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("running doc['num1'].value > 1");
        SearchResponse response = client.prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(), scriptFilter("doc['num1'].value > 1")))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", "doc['num1'].value")
                .execute().actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        assertThat(response.hits().getAt(0).id(), equalTo("2"));
        assertThat((Double) response.hits().getAt(0).fields().get("sNum1").values().get(0), equalTo(2.0));
        assertThat(response.hits().getAt(1).id(), equalTo("3"));
        assertThat((Double) response.hits().getAt(1).fields().get("sNum1").values().get(0), equalTo(3.0));

        logger.info("running doc['num1'].value > param1");
        response = client.prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(), scriptFilter("doc['num1'].value > param1").addParam("param1", 2)))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", "doc['num1'].value")
                .execute().actionGet();

        assertThat(response.hits().totalHits(), equalTo(1l));
        assertThat(response.hits().getAt(0).id(), equalTo("3"));
        assertThat((Double) response.hits().getAt(0).fields().get("sNum1").values().get(0), equalTo(3.0));

        logger.info("running doc['num1'].value > param1");
        response = client.prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(), scriptFilter("doc['num1'].value > param1").addParam("param1", -1)))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", "doc['num1'].value")
                .execute().actionGet();

        assertThat(response.hits().totalHits(), equalTo(3l));
        assertThat(response.hits().getAt(0).id(), equalTo("1"));
        assertThat((Double) response.hits().getAt(0).fields().get("sNum1").values().get(0), equalTo(1.0));
        assertThat(response.hits().getAt(1).id(), equalTo("2"));
        assertThat((Double) response.hits().getAt(1).fields().get("sNum1").values().get(0), equalTo(2.0));
        assertThat(response.hits().getAt(2).id(), equalTo("3"));
        assertThat((Double) response.hits().getAt(2).fields().get("sNum1").values().get(0), equalTo(3.0));
    }

    private static AtomicInteger scriptCounter = new AtomicInteger(0);

    public static int incrementScriptCounter() {
        return scriptCounter.incrementAndGet();
    }

    @Test
    public void testCustomScriptCache() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception ex) {
            //
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("test", "1").field("num", 1.0f).endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("test", "2").field("num", 2.0f).endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject().field("test", "3").field("num", 3.0f).endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.admin().indices().refresh(refreshRequest()).actionGet();

        String script = "org.elasticsearch.test.integration.search.scriptfilter.ScriptFilterSearchTests.incrementScriptCounter() > 0";

        scriptCounter.set(0);
        logger.info("running script filter the first time");
        SearchResponse response = client.prepareSearch()
                .setQuery(filteredQuery(termQuery("test", "1"), scriptFilter(script).cache(true)))
                .execute().actionGet();

        assertThat(response.hits().totalHits(), equalTo(1l));
        assertThat(scriptCounter.get(), equalTo(3));

        scriptCounter.set(0);
        logger.info("running script filter the second time");
        response = client.prepareSearch()
                .setQuery(filteredQuery(termQuery("test", "2"), scriptFilter(script).cache(true)))
                .execute().actionGet();

        assertThat(response.hits().totalHits(), equalTo(1l));
        assertThat(scriptCounter.get(), equalTo(0));

        scriptCounter.set(0);
        logger.info("running script filter with new parameters");
        response = client.prepareSearch()
                .setQuery(filteredQuery(termQuery("test", "1"), scriptFilter(script).addParam("param1", "1").cache(true)))
                .execute().actionGet();

        assertThat(response.hits().totalHits(), equalTo(1l));
        assertThat(scriptCounter.get(), equalTo(3));

        scriptCounter.set(0);
        logger.info("running script filter with same parameters");
        response = client.prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(), scriptFilter(script).addParam("param1", "1").cache(true)))
                .execute().actionGet();

        assertThat(response.hits().totalHits(), equalTo(3l));
        assertThat(scriptCounter.get(), equalTo(0));
    }
}