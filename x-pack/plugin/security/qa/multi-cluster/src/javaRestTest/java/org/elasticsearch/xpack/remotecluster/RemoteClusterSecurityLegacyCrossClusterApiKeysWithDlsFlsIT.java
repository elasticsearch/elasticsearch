/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyResponse;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityLegacyCrossClusterApiKeysWithDlsFlsIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();
    private static final AtomicBoolean SSL_ENABLED_REF = new AtomicBoolean();
    private static final AtomicBoolean NODE1_RCS_SERVER_ENABLED = new AtomicBoolean();
    private static final AtomicBoolean NODE2_RCS_SERVER_ENABLED = new AtomicBoolean();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("fulfilling-cluster")
            .nodes(3)
            .apply(commonClusterConfig)
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", () -> String.valueOf(SSL_ENABLED_REF.get()))
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .node(0, spec -> spec.setting("remote_cluster_server.enabled", "true"))
            .node(1, spec -> spec.setting("remote_cluster_server.enabled", () -> String.valueOf(NODE1_RCS_SERVER_ENABLED.get())))
            .node(2, spec -> spec.setting("remote_cluster_server.enabled", () -> String.valueOf(NODE2_RCS_SERVER_ENABLED.get())))
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", () -> String.valueOf(SSL_ENABLED_REF.get()))
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                if (API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
                        {
                          "search": [
                            {
                                "names": ["shared-metrics"]
                            }
                          ],
                          "replication": [
                            {
                                "names": ["shared-metrics"]
                            }
                          ]
                        }""");
                    API_KEY_MAP_REF.set(apiKeyMap);
                }
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user(REMOTE_METRIC_USER, PASS.toString(), "read_remote_shared_metrics", false)
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    // `SSL_ENABLED_REF` is used to control the SSL-enabled setting on the test clusters
    // We set it here, since randomization methods are not available in the static initialize context above
    public static TestRule clusterRule = RuleChain.outerRule(new RunnableTestRuleAdapter(() -> {
        SSL_ENABLED_REF.set(usually());
        NODE1_RCS_SERVER_ENABLED.set(randomBoolean());
        NODE2_RCS_SERVER_ENABLED.set(randomBoolean());
    })).around(fulfillingCluster).around(queryCluster);

    public void testCrossClusterSearchBlockedIfApiKeyInvalid() throws Exception {
        configureRemoteCluster();
        final String crossClusterAccessApiKeyId = (String) API_KEY_MAP_REF.get().get("id");

        // Fulfilling cluster
        {
            // Spread the shards to all nodes
            final Request createIndexRequest = new Request("PUT", "shared-metrics");
            createIndexRequest.setJsonEntity("""
                {
                  "settings": {
                    "number_of_shards": 3,
                    "number_of_replicas": 0
                  }
                }""");
            assertOK(performRequestAgainstFulfillingCluster(createIndexRequest));

            // Index some documents, so we can attempt to search them from the querying cluster
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric1" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric2" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric3" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric4" }
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        // Query cluster -- test searching works (the API key is valid)
        {
            final var searchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s:%s/_search?ccs_minimize_roundtrips=%s",
                    randomFrom("my_remote_cluster", "*", "my_remote_*"),
                    randomFrom("shared-metrics", "*"),
                    randomBoolean()
                )
            );
            final Response response = performRequestWithRemoteMetricUser(searchRequest);
            assertOK(response);
            final SearchResponse searchResponse = SearchResponseUtils.parseSearchResponse(responseAsParser(response));
            try {
                final List<String> actualIndices = Arrays.stream(searchResponse.getHits().getHits())
                    .map(SearchHit::getIndex)
                    .collect(Collectors.toList());
                assertThat(Set.copyOf(actualIndices), containsInAnyOrder("shared-metrics"));
            } finally {
                searchResponse.decRef();
            }
        }

        // make API key invalid
        addDlsQueryToApiKeyDoc(crossClusterAccessApiKeyId);

        // since we updated the API key doc directly, caches need to be clearer manually -- this would also happen during a rolling restart
        // to the FC, during an upgrade
        assertOK(performRequestAgainstFulfillingCluster(new Request("POST", "/_security/role/*/_clear_cache")));
        assertOK(performRequestAgainstFulfillingCluster(new Request("POST", "/_security/api_key/*/_clear_cache")));

        // check that GET still works
        getCrossClusterApiKeys(crossClusterAccessApiKeyId);
        {
            final var searchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s:%s/_search?ccs_minimize_roundtrips=%s",
                    "my_remote_cluster",
                    "shared-metrics",
                    randomBoolean()
                )
            );
            // TODO test skip_unavailable set and not set
            final ResponseException ex = expectThrows(ResponseException.class, () -> performRequestWithRemoteMetricUser(searchRequest));
            assertThat(
                ex.getMessage(),
                containsString("search does not support document or field level security if replication is assigned")
            );
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        }
    }

    // TODO a test for CCR follow (and ideally auto-follow) -- however, this is less critical since CCR will simply ignore DLS/FLS so the
    // it's less important to test that it's blocked

    private void addDlsQueryToApiKeyDoc(String crossClusterAccessApiKeyId) throws IOException {
        var getCrossClusterApiKeysResponse = getCrossClusterApiKeys(crossClusterAccessApiKeyId);
        assertThat(getCrossClusterApiKeysResponse.getApiKeyInfoList().size(), equalTo(1));
        var apiKey = getCrossClusterApiKeysResponse.getApiKeyInfoList().get(0);
        assertThat(apiKey.apiKeyInfo().getRoleDescriptors().size(), equalTo(1));
        var rd = apiKey.apiKeyInfo().getRoleDescriptors().get(0);
        for (var ip : rd.getIndicesPrivileges()) {
            if (Arrays.equals(ip.getPrivileges(), CrossClusterApiKeyRoleDescriptorBuilder.CCS_INDICES_PRIVILEGE_NAMES)) {
                ip.setQuery(new BytesArray("{\"match_all\": {}}"));
            }
        }
        var builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field(rd.getName(), (contentBuilder, params) -> rd.toXContent(contentBuilder, params, true));
        builder.endObject();
        updateApiKey(crossClusterAccessApiKeyId, org.elasticsearch.common.Strings.toString(builder));
    }

    private GetApiKeyResponse getCrossClusterApiKeys(String id) throws IOException {
        // TODO also test that Query works (we need to make sure that invalid API keys are still returned in Get and Query so that
        // users can view them in the UI
        final var request = new Request(HttpGet.METHOD_NAME, "/_security/api_key");
        request.addParameters(Map.of("id", id));
        return GetApiKeyResponse.fromXContent(getParser(performRequestAgainstFulfillingCluster(request)));
    }

    private static XContentParser getParser(Response response) throws IOException {
        final byte[] responseBody = EntityUtils.toByteArray(response.getEntity());
        return XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, responseBody);
    }

    static void updateApiKey(String id, String payload) throws IOException {
        final Request request = new Request("POST", "/.security/_update/" + id + "?refresh=true");
        request.setJsonEntity(Strings.format("""
            {
              "doc": {
                "role_descriptors": %s
              }
            }
            """, payload));
        expectWarnings(
            request,
            "this request accesses system indices: [.security-7],"
                + " but in a future major version, direct access to system indices will be prevented by default"
        );
        Response response = performRequestAgainstFulfillingCluster(request);
        assertOK(response);
    }

    private Response performRequestWithRemoteMetricUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_METRIC_USER, PASS))
        );
        return client().performRequest(request);
    }

    static void expectWarnings(Request request, String... expectedWarnings) {
        final Set<String> expected = Set.of(expectedWarnings);
        RequestOptions options = request.getOptions().toBuilder().setWarningsHandler(warnings -> {
            final Set<String> actual = Set.copyOf(warnings);
            // Return true if the warnings aren't what we expected; the client will treat them as a fatal error.
            return actual.equals(expected) == false;
        }).build();
        request.setOptions(options);
    }
}
