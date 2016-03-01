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

package org.elasticsearch.plugin.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESClientTestCase;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Example integration test against an external client.
 */
public class ExampleExternalIT extends ESClientTestCase {
    public void testExampleFixture() throws Exception {
        String stringAddress = Objects.requireNonNull(System.getProperty("fixture.address"));
        URL url = new URL("http://" + stringAddress);
        InetAddress address = InetAddress.getByName(url.getHost());
        try (Socket socket = new Socket(address, url.getPort());
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
           assertEquals("TEST", reader.readLine());
        }
    }

    public void testSimpleClient() {
        Client client = getClient();

        ClusterHealthResponse health = client.admin().cluster().prepareHealth().setWaitForYellowStatus().get();
        String clusterName = health.getClusterName();
        int numberOfNodes = health.getNumberOfNodes();
        assertThat("cluster [" + clusterName + "] should have at least 1 node", numberOfNodes, greaterThan(0));
    }
}
