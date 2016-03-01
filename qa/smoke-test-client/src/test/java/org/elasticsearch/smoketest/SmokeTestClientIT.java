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

package org.elasticsearch.smoketest;

import java.util.Locale;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESClientTestCase;
import org.junit.After;
import org.junit.Before;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiOfLength;

public class SmokeTestClientIT extends ESClientTestCase {
    protected String index;

    @Before
    public void defineIndexName() {
        doClean();
        index = "qa-smoke-test-client-" + randomAsciiOfLength(10).toLowerCase(Locale.getDefault());
    }

    @After
    public void cleanIndex() {
        doClean();
    }

    private void doClean() {
        if (getClientNoCreate() != null) {
            try {
                getClientNoCreate().admin().indices().prepareDelete(index).get();
            } catch (Exception e) {
                // We ignore this cleanup exception
            }
        }
    }

    /**
     * Check that we are connected to a cluster.
     */
    public void testSimpleClient() {
        Client client = getClient();

        // START SNIPPET: java-doc-admin-cluster-health
        ClusterHealthResponse health = client.admin().cluster().prepareHealth().setWaitForYellowStatus().get();
        String clusterName = health.getClusterName();
        int numberOfNodes = health.getNumberOfNodes();
        // END SNIPPET: java-doc-admin-cluster-health
        assertThat("cluster [" + clusterName + "] should have at least 1 node", numberOfNodes, greaterThan(0));
    }

    /**
     * Create an index and index some docs
     */
    public void testPutDocument() {
        Client client = getClient();

        // START SNIPPET: java-doc-index-doc-simple
        client.prepareIndex(index, "doc", "1")  // Index, Type, Id
                .setSource("foo", "bar")        // Simple document: { "foo" : "bar" }
                .get();                         // Execute and wait for the result
        // END SNIPPET: java-doc-index-doc-simple

        // START SNIPPET: java-doc-admin-indices-refresh
        // Prepare a refresh action on a given index, execute and wait for the result
        client.admin().indices().prepareRefresh(index).get();
        // END SNIPPET: java-doc-admin-indices-refresh

        // START SNIPPET: java-doc-search-simple
        SearchResponse searchResponse = client.prepareSearch(index).get();
        assertThat(searchResponse.getHits().getTotalHits(), is(1L));
        // END SNIPPET: java-doc-search-simple
    }
}

