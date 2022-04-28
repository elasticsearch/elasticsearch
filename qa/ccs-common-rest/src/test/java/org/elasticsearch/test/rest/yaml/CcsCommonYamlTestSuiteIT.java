/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Version;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.elasticsearch.test.rest.yaml.section.MatchAssertion;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;

@TimeoutSuite(millis = 15 * TimeUnits.MINUTE) // to account for slow as hell VMs
public class CcsCommonYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final Logger logger = LogManager.getLogger(CcsCommonYamlTestSuiteIT.class);
    private static RestClient searchClient;
    private static RestClient adminSearchClient;
    private static List<HttpHost> clusterHosts;
    private static ClientYamlTestClient searchYamlTestClient;

    private static final String REMOTE_CLUSTER_NAME = "remote_cluster";

    @Before
    public void initSearchClient() throws IOException {
        if (searchClient == null) {
            assert adminSearchClient == null;
            assert clusterHosts == null;

            final String cluster = System.getProperty("tests.rest.search_cluster");
            assertNotNull("[tests.rest.search_cluster] is not configured", cluster);
            String[] stringUrls = cluster.split(",");
            List<HttpHost> hosts = new ArrayList<>(stringUrls.length);
            for (String stringUrl : stringUrls) {
                int portSeparator = stringUrl.lastIndexOf(':');
                if (portSeparator < 0) {
                    throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
                }
                String host = stringUrl.substring(0, portSeparator);
                int port = Integer.parseInt(stringUrl.substring(portSeparator + 1));
                hosts.add(buildHttpHost(host, port));
            }
            clusterHosts = unmodifiableList(hosts);
            logger.info("initializing REST search clients against {}", clusterHosts);
            searchClient = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[clusterHosts.size()]));
            adminSearchClient = buildClient(restAdminSettings(), clusterHosts.toArray(new HttpHost[clusterHosts.size()]));

            Tuple<Version, Version> versionVersionTuple = readVersionsFromCatNodes(adminSearchClient);
            final Version esVersion = versionVersionTuple.v1();
            final Version masterVersion = versionVersionTuple.v2();
            final String os = readOsFromNodesInfo(adminSearchClient);

            searchYamlTestClient = new ClientYamlTestClient(
                getRestSpec(),
                searchClient,
                hosts,
                esVersion,
                masterVersion,
                os,
                this::getClientBuilderWithSniffedHosts
            ) {
                public ClientYamlTestResponse callApi(
                    String apiName,
                    Map<String, String> params,
                    HttpEntity entity,
                    Map<String, String> headers,
                    NodeSelector nodeSelector
                ) throws IOException {
                    // on request, we need to replace index specifications by prefixing the remote cluster
                    String originalIndices = params.get("index");
                    String expandedIndices = REMOTE_CLUSTER_NAME + ":*";
                    if (originalIndices != null && (originalIndices.isEmpty() == false)) {
                        String[] indices = originalIndices.split(",");
                        List<String> newIndices = new ArrayList<>();
                        for (String indexName : indices) {
                            newIndices.add(REMOTE_CLUSTER_NAME + ":" + indexName);
                        }
                        expandedIndices = String.join(",", newIndices);
                    }
                    params.put("index", String.join(",", expandedIndices));
                    ClientYamlTestResponse clientYamlTestResponse = super.callApi(apiName, params, entity, headers, nodeSelector);
                    return clientYamlTestResponse;
                };
            };

            // check that we have an established CCS connection
            Request request = new Request("GET", "_remote/info");
            Response response = adminSearchClient.performRequest(request);
            assertOK(response);
            ObjectPath responseObject = ObjectPath.createFromResponse(response);
            assertNotNull(responseObject.evaluate(REMOTE_CLUSTER_NAME));
            logger.info("Established connection to remote cluster [" + REMOTE_CLUSTER_NAME + "]");
        }

        assert searchClient != null;
        assert adminSearchClient != null;
        assert clusterHosts != null;
    }

    public CcsCommonYamlTestSuiteIT(ClientYamlTestCandidate testCandidate) throws IOException {
        super(rewrite(testCandidate));
    }

    private static ClientYamlTestCandidate rewrite(ClientYamlTestCandidate clientYamlTestCandidate) {
        ClientYamlTestSection testSection = clientYamlTestCandidate.getTestSection();
        List<ExecutableSection> executableSections = testSection.getExecutableSections();
        List<ExecutableSection> modifiedExecutableSections = new ArrayList<>();
        for (ExecutableSection section : executableSections) {
            if (section instanceof MatchAssertion) {
                MatchAssertion matchSection = (MatchAssertion) section;
                if (matchSection.getField().endsWith("_index")) {
                    String modifiedExpectedValue = REMOTE_CLUSTER_NAME + ":" + matchSection.getExpectedValue();
                    modifiedExecutableSections.add(
                        new MatchAssertion(matchSection.getLocation(), matchSection.getField(), modifiedExpectedValue)
                    );
                }
            } else {
                modifiedExecutableSections.add(section);
            }
        }
        return new ClientYamlTestCandidate(
            clientYamlTestCandidate.getRestTestSuite(),
            new ClientYamlTestSection(
                testSection.getLocation(),
                testSection.getName(),
                testSection.getSkipSection(),
                modifiedExecutableSections
            )
        );
    };

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected ClientYamlTestExecutionContext createRestTestExecutionContext(
        ClientYamlTestCandidate clientYamlTestCandidate,
        ClientYamlTestClient clientYamlTestClient
    ) {
        return new ClientYamlTestExecutionContext(clientYamlTestCandidate, clientYamlTestClient, randomizeContentType()) {
            protected ClientYamlTestClient clientYamlTestClient(String apiName) {
                if (apiName.equals("search") || apiName.equals("field_caps")) {
                    return searchYamlTestClient;
                } else {
                    return super.clientYamlTestClient(apiName);
                }
            }
        };
    }

    @AfterClass
    public static void closeSearchClients() throws IOException {
        try {
            IOUtils.close(searchClient, adminSearchClient);
        } finally {
            clusterHosts = null;
        }
    }
}
