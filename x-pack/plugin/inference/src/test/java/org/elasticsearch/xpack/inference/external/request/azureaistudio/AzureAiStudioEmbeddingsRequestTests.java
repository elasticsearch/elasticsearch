/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureaistudio;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioDeploymentType;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsModelTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiUtils.API_KEY_HEADER;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AzureAiStudioEmbeddingsRequestTests extends ESTestCase {


    private static final String DEFAULT_TARGET = "http://target.local/embeddings";
    private static final String DEFAULT_MODEL = "test-model";

    public void testCreateRequest_WithModelInferenceServiceDeployment_NoAdditionalParams() throws IOException {
        var request = createRequest(
            DEFAULT_TARGET,
            AzureAiStudioDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            "apikey",
            "abcd",
            null
        );
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest, DEFAULT_TARGET);
        validateRequestApiKey(httpPost, AzureAiStudioDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("input"), is(List.of("abcd")));
        assertThat(requestMap.get("model"), is(DEFAULT_MODEL));
    }

    public void testCreateRequest_WithServerlessDeployment_NoAdditionalParams() throws IOException {
        var request = createRequest(
            DEFAULT_TARGET,
            AzureAiStudioDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            "apikey",
            "abcd",
            null
        );

        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest, DEFAULT_TARGET);
        validateRequestApiKey(httpPost, AzureAiStudioDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("input"), is(List.of("abcd")));
        assertThat(requestMap.get("model"), is(DEFAULT_MODEL));
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var request = createRequest(
            DEFAULT_TARGET,
            AzureAiStudioDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            "apikey",
            "abcd",
            null
        );
        var truncatedRequest = request.truncate();

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("input"), is(List.of("ab")));
        assertThat(requestMap.get("model"), is(DEFAULT_MODEL));
    }

    public void testIsTruncated_ReturnsTrue() {
        var request = createRequest(
            DEFAULT_TARGET,
            AzureAiStudioDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            "apikey",
            "abcd",
            null
        );
        assertFalse(request.getTruncationInfo()[0]);

        var truncatedRequest = request.truncate();
        assertTrue(truncatedRequest.getTruncationInfo()[0]);
    }

    private HttpPost validateRequestUrlAndContentType(HttpRequest request, String expectedUrl) {
        assertThat(request.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) request.httpRequestBase();
        assertThat(httpPost.getURI().toString(), is(expectedUrl));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        return httpPost;
    }

    private void validateRequestApiKey(HttpPost httpPost, AzureAiStudioDeploymentType deploymentType, String apiKey) {
        if (deploymentType == AzureAiStudioDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE) {
            assertThat(httpPost.getLastHeader(API_KEY_HEADER).getValue(), is(apiKey));
        } else if (deploymentType == AzureAiStudioDeploymentType.SERVERLESS_API) {
            assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(apiKey));
        } else {
            fail("Invalid deployment type");
        }
    }

    public static AzureAiStudioEmbeddingsRequest createRequest(
        String target,
        AzureAiStudioDeploymentType deploymentType,
        String deploymentName,
        String apiKey,
        String input,
        @Nullable String user
    ) {
        var model = AzureAiStudioEmbeddingsModelTests.createModel(
            "id",
            target,
            deploymentType,
            deploymentName,
            apiKey,
            null,
            false,
            null,
            null,
            user,
            null
        );
        return new AzureAiStudioEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(input), new boolean[] { false }),
            model
        );
    }
}
