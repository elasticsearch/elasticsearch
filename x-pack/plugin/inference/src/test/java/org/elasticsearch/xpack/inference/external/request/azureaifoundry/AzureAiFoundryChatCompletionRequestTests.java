/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureaifoundry;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryDeploymentType;
import org.elasticsearch.xpack.inference.services.azureaifoundry.completion.AzureAiFoundryChatCompletionModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiUtils.API_KEY_HEADER;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AzureAiFoundryChatCompletionRequestTests extends ESTestCase {

    private static final String DEFAULT_TARGET = "http://target.local/completions";
    private static final String DEFAULT_MODEL = "test-model";
    private static final String BEARER_PREFIX = "Bearer ";

    public void testCreateRequest_WithModelInferenceServiceDeployment_NoParams() throws IOException {
        var request = createRequest(
            DEFAULT_TARGET,
            AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            "apikey",
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, DEFAULT_TARGET);
        validateRequestApiKey(httpPost, AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("model"), is(DEFAULT_MODEL));
    }

    public void testCreateRequest_WithModelInferenceServiceDeployment_WithTemperatureParam() throws IOException {
        var request = createRequest(
            DEFAULT_TARGET,
            AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            "apikey",
            1.0,
            null,
            null,
            null,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, DEFAULT_TARGET);
        validateRequestApiKey(httpPost, AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("temperature"), is(1.0));
        assertThat(requestMap.get("model"), is(DEFAULT_MODEL));
    }

    public void testCreateRequest_WithModelInferenceServiceDeployment_WithTopPParam() throws IOException {
        var request = createRequest(
            DEFAULT_TARGET,
            AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            "apikey",
            null,
            2.0,
            null,
            null,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, DEFAULT_TARGET);
        validateRequestApiKey(httpPost, AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("top_p"), is(2.0));
        assertThat(requestMap.get("model"), is(DEFAULT_MODEL));
    }

    public void testCreateRequest_WithModelInferenceServiceDeployment_WithDoSampleParam() throws IOException {
        var request = createRequest(
            DEFAULT_TARGET,
            AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            "apikey",
            null,
            null,
            true,
            null,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, DEFAULT_TARGET);
        validateRequestApiKey(httpPost, AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("do_sample"), is(true));
        assertThat(requestMap.get("model"), is(DEFAULT_MODEL));
    }

    public void testCreateRequest_WithModelInferenceServiceDeployment_WithMaxTokensParam() throws IOException {
        var request = createRequest(
            DEFAULT_TARGET,
            AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            "apikey",
            null,
            null,
            null,
            512,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, DEFAULT_TARGET);
        validateRequestApiKey(httpPost, AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("max_tokens"), is(512));
        assertThat(requestMap.get("model"), is(DEFAULT_MODEL));
    }

    public void testCreateRequest_WithServerlessDeployment_NoParams() throws IOException {
        var request = createRequest(
            DEFAULT_TARGET,
            AzureAiFoundryDeploymentType.SERVERLESS_API,
            null,
            "apikey",
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, DEFAULT_TARGET);
        validateRequestApiKey(httpPost, AzureAiFoundryDeploymentType.SERVERLESS_API, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
    }

    public void testCreateRequest_WithServerlessDeployment_WithModel() throws IOException {
        var request = createRequest(
            DEFAULT_TARGET,
            AzureAiFoundryDeploymentType.SERVERLESS_API,
            DEFAULT_MODEL,
            "apikey",
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, DEFAULT_TARGET);
        validateRequestApiKey(httpPost, AzureAiFoundryDeploymentType.SERVERLESS_API, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
    }

    public void testCreateRequest_WithServerlessDeployment_WithTemperatureParam() throws IOException {
        var request = createRequest(
            DEFAULT_TARGET,
            AzureAiFoundryDeploymentType.SERVERLESS_API,
            null,
            "apikey",
            1.0,
            null,
            null,
            null,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, DEFAULT_TARGET);
        validateRequestApiKey(httpPost, AzureAiFoundryDeploymentType.SERVERLESS_API, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("temperature"), is(1.0));
    }

    public void testCreateRequest_WithServerlessDeployment_WithTopPParam() throws IOException {
        var request = createRequest(
            DEFAULT_TARGET,
            AzureAiFoundryDeploymentType.SERVERLESS_API,
            null,
            "apikey",
            null,
            2.0,
            null,
            null,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, DEFAULT_TARGET);
        validateRequestApiKey(httpPost, AzureAiFoundryDeploymentType.SERVERLESS_API, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
    }

    public void testCreateRequest_WithServerlessDeployment_WithDoSampleParam() throws IOException {
        var request = createRequest(
            DEFAULT_TARGET,
            AzureAiFoundryDeploymentType.SERVERLESS_API,
            null,
            "apikey",
            null,
            null,
            true,
            null,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, DEFAULT_TARGET);
        validateRequestApiKey(httpPost, AzureAiFoundryDeploymentType.SERVERLESS_API, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("do_sample"), is(true));
    }

    public void testCreateRequest_WithServerlessDeployment_WithMaxTokensParam() throws IOException {
        var request = createRequest(
            DEFAULT_TARGET,
            AzureAiFoundryDeploymentType.SERVERLESS_API,
            null,
            "apikey",
            null,
            null,
            null,
            512,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, DEFAULT_TARGET);
        validateRequestApiKey(httpPost, AzureAiFoundryDeploymentType.SERVERLESS_API, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("max_tokens"), is(512));
    }

    private HttpPost validateRequestUrlAndContentType(HttpRequest request, String expectedUrl) {
        assertThat(request.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) request.httpRequestBase();
        assertThat(httpPost.getURI().toString(), is(expectedUrl));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        return httpPost;
    }

    private void validateRequestApiKey(HttpPost httpPost, AzureAiFoundryDeploymentType deploymentType, String apiKey
    ) {
        if (deploymentType == AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE) {
            assertThat(httpPost.getLastHeader(API_KEY_HEADER).getValue(), is(apiKey));
        } else if (deploymentType == AzureAiFoundryDeploymentType.SERVERLESS_API) {
            assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(BEARER_PREFIX + apiKey));
        } else {
            fail("Invalid deployment type");
        }
    }

    public static AzureAiFoundryChatCompletionRequest createRequest(
        String target,
        AzureAiFoundryDeploymentType deploymentType,
        @Nullable String model,
        String apiKey,
        String input
    ) {
        return createRequest(target, deploymentType, model, apiKey, null, null, null, null, input);
    }

    public static AzureAiFoundryChatCompletionRequest createRequest(
        String target,
        AzureAiFoundryDeploymentType deploymentType,
        @Nullable String model,
        String apiKey,
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Boolean doSample,
        @Nullable Integer maxTokens,
        String input
    ) {
        var completionModel = AzureAiFoundryChatCompletionModelTests.createModel(
            "id",
            target,
            deploymentType,
            model,
            apiKey,
            temperature,
            topP,
            doSample,
            maxTokens,
            null
        );
        return new AzureAiFoundryChatCompletionRequest(completionModel, List.of(input), false);
    }
}
