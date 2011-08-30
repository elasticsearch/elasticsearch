/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.test.integration.timestamp;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 */
public class SimpleTimestampTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass public void createNodes() throws Exception {
        startNode("node1");
        startNode("node2");
        client = getClient();
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test public void testSimpleTimestamp() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("_timestamp").field("enabled", true).field("store", "yes").endObject().endObject().endObject())
                .execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        logger.info("--> check with automatic timestamp");
        long now1 = System.currentTimeMillis();
        client.prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();
        long now2 = System.currentTimeMillis();

        // we check both realtime get and non realtime get
        GetResponse getResponse = client.prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(true).execute().actionGet();
        long timestamp = ((Number) getResponse.field("_timestamp").value()).longValue();
        assertThat(timestamp, greaterThanOrEqualTo(now1));
        assertThat(timestamp, lessThanOrEqualTo(now2));
        // verify its the same timestamp when going the replica
        getResponse = client.prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(true).execute().actionGet();
        assertThat(((Number) getResponse.field("_timestamp").value()).longValue(), equalTo(timestamp));

        // non realtime get (stored)
        getResponse = client.prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(false).execute().actionGet();
        timestamp = ((Number) getResponse.field("_timestamp").value()).longValue();
        assertThat(timestamp, greaterThanOrEqualTo(now1));
        assertThat(timestamp, lessThanOrEqualTo(now2));
        // verify its the same timestamp when going the replica
        getResponse = client.prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(false).execute().actionGet();
        assertThat(((Number) getResponse.field("_timestamp").value()).longValue(), equalTo(timestamp));

        logger.info("--> check with custom timestamp (numeric)");
        client.prepareIndex("test", "type1", "1").setSource("field1", "value1").setTimestamp("10").setRefresh(true).execute().actionGet();

        getResponse = client.prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(false).execute().actionGet();
        timestamp = ((Number) getResponse.field("_timestamp").value()).longValue();
        assertThat(timestamp, equalTo(10l));
        // verify its the same timestamp when going the replica
        getResponse = client.prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(false).execute().actionGet();
        assertThat(((Number) getResponse.field("_timestamp").value()).longValue(), equalTo(timestamp));

        logger.info("--> check with custom timestamp (string)");
        client.prepareIndex("test", "type1", "1").setSource("field1", "value1").setTimestamp("1970-01-01T00:00:00.020").setRefresh(true).execute().actionGet();

        getResponse = client.prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(false).execute().actionGet();
        timestamp = ((Number) getResponse.field("_timestamp").value()).longValue();
        assertThat(timestamp, equalTo(20l));
        // verify its the same timestamp when going the replica
        getResponse = client.prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(false).execute().actionGet();
        assertThat(((Number) getResponse.field("_timestamp").value()).longValue(), equalTo(timestamp));
    }
}
