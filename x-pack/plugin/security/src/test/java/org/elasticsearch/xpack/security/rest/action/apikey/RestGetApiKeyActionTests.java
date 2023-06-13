/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKeyTests;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests.randomUniquelyNamedRoleDescriptors;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class RestGetApiKeyActionTests extends ESTestCase {
    private final XPackLicenseState mockLicenseState = mock(XPackLicenseState.class);
    private Settings settings = null;
    private ThreadPool threadPool = null;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .put("node.name", "test-" + getTestName())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        threadPool = new ThreadPool(settings);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testGetApiKey() throws Exception {
        final Map<String, String> param1 = Map.of("realm_name", "realm-1", "username", "user-x");
        final Map<String, String> param2 = Map.of("realm_name", "realm-1");
        final Map<String, String> param3 = Map.of("username", "user-x");
        final Map<String, String> param4 = Map.of("id", "api-key-id-1");
        final Map<String, String> param5 = Map.of("name", "api-key-name-1");
        final Map<String, String> params = new HashMap<>(randomFrom(param1, param2, param3, param4, param5));
        final boolean withLimitedBy = randomBoolean();
        if (withLimitedBy) {
            params.put("with_limited_by", "true");
        } else {
            if (randomBoolean()) {
                params.put("with_limited_by", "false");
            }
        }
        final boolean replyEmptyResponse = rarely();
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(params).build();

        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        final RestChannel restChannel = new AbstractRestChannel(restRequest, randomBoolean()) {
            @Override
            public void sendResponse(RestResponse restResponse) {
                responseSetOnce.set(restResponse);
            }
        };
        final ApiKey.Type type = TcpTransport.isUntrustedRemoteClusterEnabled() ? randomFrom(ApiKey.Type.values()) : ApiKey.Type.REST;
        final Instant creation = Instant.now();
        final Instant expiration = randomFrom(Arrays.asList(null, Instant.now().plus(10, ChronoUnit.DAYS)));
        final Map<String, Object> metadata = ApiKeyTests.randomMetadata();
        final List<RoleDescriptor> roleDescriptors = randomUniquelyNamedRoleDescriptors(0, 3);
        final List<RoleDescriptor> limitedByRoleDescriptors = withLimitedBy && type != ApiKey.Type.CROSS_CLUSTER
            ? randomUniquelyNamedRoleDescriptors(1, 3)
            : null;
        final GetApiKeyResponse getApiKeyResponseExpected = new GetApiKeyResponse(
            Collections.singletonList(
                new ApiKey(
                    "api-key-name-1",
                    "api-key-id-1",
                    type,
                    creation,
                    expiration,
                    false,
                    "user-x",
                    "realm-1",
                    metadata,
                    roleDescriptors,
                    limitedByRoleDescriptors
                )
            )
        );

        try (NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @SuppressWarnings("unchecked")
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                GetApiKeyRequest getApiKeyRequest = (GetApiKeyRequest) request;
                ActionRequestValidationException validationException = getApiKeyRequest.validate();
                if (validationException != null) {
                    listener.onFailure(validationException);
                    return;
                }
                if (getApiKeyRequest.getApiKeyName() != null && getApiKeyRequest.getApiKeyName().equals("api-key-name-1")
                    || getApiKeyRequest.getApiKeyId() != null && getApiKeyRequest.getApiKeyId().equals("api-key-id-1")
                    || getApiKeyRequest.getRealmName() != null && getApiKeyRequest.getRealmName().equals("realm-1")
                    || getApiKeyRequest.getUserName() != null && getApiKeyRequest.getUserName().equals("user-x")) {
                    if (replyEmptyResponse) {
                        listener.onResponse((Response) GetApiKeyResponse.emptyResponse());
                    } else {
                        listener.onResponse((Response) getApiKeyResponseExpected);
                    }
                } else {
                    listener.onFailure(new ElasticsearchSecurityException("encountered an error while creating API key"));
                }
            }
        }) {
            final RestGetApiKeyAction restGetApiKeyAction = new RestGetApiKeyAction(Settings.EMPTY, mockLicenseState);

            restGetApiKeyAction.handleRequest(restRequest, restChannel, client);

            final RestResponse restResponse = responseSetOnce.get();
            assertNotNull(restResponse);
            assertThat(
                restResponse.status(),
                (replyEmptyResponse && params.get("id") != null) ? is(RestStatus.NOT_FOUND) : is(RestStatus.OK)
            );
            final GetApiKeyResponse actual = GetApiKeyResponse.fromXContent(
                createParser(XContentType.JSON.xContent(), restResponse.content())
            );
            if (replyEmptyResponse) {
                assertThat(actual.getApiKeyInfos().length, is(0));
            } else {
                assertThat(
                    actual.getApiKeyInfos(),
                    arrayContaining(
                        new ApiKey(
                            "api-key-name-1",
                            "api-key-id-1",
                            type,
                            creation,
                            expiration,
                            false,
                            "user-x",
                            "realm-1",
                            metadata,
                            roleDescriptors,
                            limitedByRoleDescriptors
                        )
                    )
                );
            }
        }

    }

    public void testGetApiKeyOwnedByCurrentAuthenticatedUser() throws Exception {
        final boolean isGetRequestForOwnedKeysOnly = randomBoolean();
        final Map<String, String> param = new HashMap<>();
        if (isGetRequestForOwnedKeysOnly) {
            param.put("owner", Boolean.TRUE.toString());
        } else {
            param.put("owner", Boolean.FALSE.toString());
            param.put("realm_name", "realm-1");
        }
        final boolean withLimitedBy = randomBoolean();
        if (withLimitedBy) {
            param.put("with_limited_by", "true");
        } else {
            if (randomBoolean()) {
                param.put("with_limited_by", "false");
            }
        }

        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(param).build();

        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        final RestChannel restChannel = new AbstractRestChannel(restRequest, randomBoolean()) {
            @Override
            public void sendResponse(RestResponse restResponse) {
                responseSetOnce.set(restResponse);
            }
        };

        final ApiKey.Type type = TcpTransport.isUntrustedRemoteClusterEnabled() ? randomFrom(ApiKey.Type.values()) : ApiKey.Type.REST;
        final Instant creation = Instant.now();
        final Instant expiration = randomFrom(Arrays.asList(null, Instant.now().plus(10, ChronoUnit.DAYS)));
        final ApiKey apiKey1 = new ApiKey(
            "api-key-name-1",
            "api-key-id-1",
            type,
            creation,
            expiration,
            false,
            "user-x",
            "realm-1",
            ApiKeyTests.randomMetadata(),
            randomUniquelyNamedRoleDescriptors(0, 3),
            withLimitedBy && type != ApiKey.Type.CROSS_CLUSTER ? randomUniquelyNamedRoleDescriptors(1, 3) : null
        );
        final ApiKey apiKey2 = new ApiKey(
            "api-key-name-2",
            "api-key-id-2",
            type,
            creation,
            expiration,
            false,
            "user-y",
            "realm-1",
            ApiKeyTests.randomMetadata(),
            randomUniquelyNamedRoleDescriptors(0, 3),
            withLimitedBy && type != ApiKey.Type.CROSS_CLUSTER ? randomUniquelyNamedRoleDescriptors(1, 3) : null
        );
        final GetApiKeyResponse getApiKeyResponseExpectedWhenOwnerFlagIsTrue = new GetApiKeyResponse(Collections.singletonList(apiKey1));
        final GetApiKeyResponse getApiKeyResponseExpectedWhenOwnerFlagIsFalse = new GetApiKeyResponse(List.of(apiKey1, apiKey2));

        try (NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @SuppressWarnings("unchecked")
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                GetApiKeyRequest getApiKeyRequest = (GetApiKeyRequest) request;
                ActionRequestValidationException validationException = getApiKeyRequest.validate();
                if (validationException != null) {
                    listener.onFailure(validationException);
                    return;
                }

                if (getApiKeyRequest.ownedByAuthenticatedUser()) {
                    listener.onResponse((Response) getApiKeyResponseExpectedWhenOwnerFlagIsTrue);
                } else if (getApiKeyRequest.getRealmName() != null && getApiKeyRequest.getRealmName().equals("realm-1")) {
                    listener.onResponse((Response) getApiKeyResponseExpectedWhenOwnerFlagIsFalse);
                }
            }
        }) {
            final RestGetApiKeyAction restGetApiKeyAction = new RestGetApiKeyAction(Settings.EMPTY, mockLicenseState);

            restGetApiKeyAction.handleRequest(restRequest, restChannel, client);

            final RestResponse restResponse = responseSetOnce.get();
            assertNotNull(restResponse);
            assertThat(restResponse.status(), is(RestStatus.OK));
            final GetApiKeyResponse actual = GetApiKeyResponse.fromXContent(
                createParser(XContentType.JSON.xContent(), restResponse.content())
            );
            if (isGetRequestForOwnedKeysOnly) {
                assertThat(actual.getApiKeyInfos().length, is(1));
                assertThat(actual.getApiKeyInfos(), arrayContaining(apiKey1));
            } else {
                assertThat(actual.getApiKeyInfos().length, is(2));
                assertThat(actual.getApiKeyInfos(), arrayContaining(apiKey1, apiKey2));
            }
        }
    }
}
