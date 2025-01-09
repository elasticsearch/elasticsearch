/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.azureaistudio;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioDeploymentType;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionModelTests;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class AzureAiStudioChatCompletionResponseEntityTests extends ESTestCase {

    public void testCompletionResponse_FromModelInferenceServiceDeployment() throws IOException {
        var entity = new AzureAiStudioChatCompletionResponseEntity();
        var model = AzureAiStudioChatCompletionModelTests.createModel(
            "id",
            "http://target.local",
            AzureAiStudioDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            "test-model",
            "apikey"
        );
        var request = new AzureAiStudioChatCompletionRequest(model, List.of("test input"), false);
        var result = (ChatCompletionResults) entity.apply(
            request,
            new HttpResult(mock(HttpResponse.class), testResponseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(result.getResults().size(), equalTo(1));
        assertThat(result.getResults().get(0).content(), is("test input string"));
    }

    public void testCompletionResponse_FromServerlessDeployment() throws IOException {
        var entity = new AzureAiStudioChatCompletionResponseEntity();
        var model = AzureAiStudioChatCompletionModelTests.createModel(
            "id",
            "http://target.local",
            AzureAiStudioDeploymentType.SERVERLESS_API,
            null,
            "apikey"
        );
        var request = new AzureAiStudioChatCompletionRequest(model, List.of("test input"), false);
        var result = (ChatCompletionResults) entity.apply(
            request,
            new HttpResult(mock(HttpResponse.class), testResponseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(result.getResults().size(), equalTo(1));
        assertThat(result.getResults().get(0).content(), is("test input string"));
    }

    private static final String testResponseJson = """
        {
            "choices": [
                {
                    "finish_reason": "stop",
                    "index": 0,
                    "message": {
                        "content": "test input string",
                        "role": "assistant",
                        "tool_calls": null
                    }
                }
            ],
            "created": 1714006424,
            "id": "f92b5b4d-0de3-4152-a3c6-5aae8a74555c",
            "model": "",
            "object": "chat.completion",
            "usage": {
                "completion_tokens": 35,
                "prompt_tokens": 8,
                "total_tokens": 43
            }
        }""";
}
