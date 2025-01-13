/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureaifoundry;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryDeploymentType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class AzureAiFoundryChatCompletionRequestEntityTests extends ESTestCase {

    private static final String DEFAULT_MODEL = "test-model";

    public void testToXContent_WhenModelInferenceServiceDeployment_NoParameters() throws IOException {
        var entity = new AzureAiFoundryChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            null,
            null,
            null,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedModelInferenceServiceDeploymentRequest(List.of("abc"), DEFAULT_MODEL, null, null, null, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenModelInferenceServiceDeployment_WithTemperatureParam() throws IOException {
        var entity = new AzureAiFoundryChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            1.0,
            null,
            null,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedModelInferenceServiceDeploymentRequest(List.of("abc"), DEFAULT_MODEL, 1.0, null, null, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenModelInferenceServiceDeployment_WithTopPParam() throws IOException {
        var entity = new AzureAiFoundryChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            null,
            2.0,
            null,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedModelInferenceServiceDeploymentRequest(List.of("abc"), DEFAULT_MODEL, null, 2.0, null, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenModelInferenceServiceDeployment_WithDoSampleParam() throws IOException {
        var entity = new AzureAiFoundryChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            null,
            null,
            true,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedModelInferenceServiceDeploymentRequest(List.of("abc"), DEFAULT_MODEL, null, null, true, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenModelInferenceServiceDeployment_WithMaxTokensParam() throws IOException {
        var entity = new AzureAiFoundryChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            null,
            null,
            null,
            512,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedModelInferenceServiceDeploymentRequest(List.of("abc"), DEFAULT_MODEL, null, null, null, 512);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenServerlessDeployment_NoParameters() throws IOException {
        var entity = new AzureAiFoundryChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiFoundryDeploymentType.SERVERLESS_API,
            null,
            null,
            null,
            null,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedServerlessDeploymentRequest(List.of("abc"), null, null, null, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenServerlessDeployment_WithTemperatureParam() throws IOException {
        var entity = new AzureAiFoundryChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiFoundryDeploymentType.SERVERLESS_API,
            null,
            1.0,
            null,
            null,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedServerlessDeploymentRequest(List.of("abc"), 1.0, null, null, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenServerlessDeployment_WithTopPParam() throws IOException {
        var entity = new AzureAiFoundryChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiFoundryDeploymentType.SERVERLESS_API,
            null,
            null,
            2.0,
            null,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedServerlessDeploymentRequest(List.of("abc"), null, 2.0, null, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenServerlessDeployment_WithDoSampleParam() throws IOException {
        var entity = new AzureAiFoundryChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiFoundryDeploymentType.SERVERLESS_API,
            null,
            null,
            null,
            true,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedServerlessDeploymentRequest(List.of("abc"), null, null, true, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenServerlessDeployment_WithMaxTokensParam() throws IOException {
        var entity = new AzureAiFoundryChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiFoundryDeploymentType.SERVERLESS_API,
            null,
            null,
            null,
            null,
            512,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedServerlessDeploymentRequest(List.of("abc"), null, null, null, 512);
        assertThat(request, is(expectedRequest));
    }

    private String getXContentAsString(AzureAiFoundryChatCompletionRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        return Strings.toString(builder);
    }

    private String getExpectedModelInferenceServiceDeploymentRequest(
        List<String> inputs,
        String model,
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Boolean doSample,
        @Nullable Integer maxTokens
    ) {
        String expected = "{";

        expected = addModel(expected, model);
        expected += ",";
        expected = addMessageInputs("messages", expected, inputs);
        expected = addParameters(expected, temperature, topP, doSample, maxTokens);

        expected += "}";
        return expected;
    }

    private String getExpectedServerlessDeploymentRequest(
        List<String> inputs,
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Boolean doSample,
        @Nullable Integer maxTokens
    ) {
        String expected = "{";

        expected = addMessageInputs("messages", expected, inputs);
        expected = addParameters(expected, temperature, topP, doSample, maxTokens);

        expected += "}";
        return expected;
    }

    private String addMessageInputs(String fieldName, String expected, List<String> inputs) {
        StringBuilder messages = new StringBuilder(Strings.format("\"%s\":[", fieldName));
        var hasOne = false;
        for (String input : inputs) {
            if (hasOne) {
                messages.append(",");
            }
            messages.append(getMessageString(input));
            hasOne = true;
        }
        messages.append("]");

        return expected + messages;
    }

    private String getMessageString(String input) {
        return Strings.format("{\"content\":\"%s\",\"role\":\"user\"}", input);
    }
    private String addModel(String expected, String model) { return expected + Strings.format("\"model\":\"%s\"", model);}
    private String addParameters(String expected, Double temperature, Double topP, Boolean doSample, Integer maxNewTokens) {
        if (temperature == null && topP == null && doSample == null && maxNewTokens == null) {
            return expected;
        }

        StringBuilder parameters = new StringBuilder(",");

        var hasOne = false;
        if (temperature != null) {
            parameters.append(Strings.format("\"temperature\":%.1f", temperature));
            hasOne = true;
        }

        if (topP != null) {
            if (hasOne) {
                parameters.append(",");
            }
            parameters.append(Strings.format("\"top_p\":%.1f", topP));
            hasOne = true;
        }

        if (doSample != null) {
            if (hasOne) {
                parameters.append(",");
            }
            parameters.append(Strings.format("\"do_sample\":%s", doSample.equals(Boolean.TRUE)));
            hasOne = true;
        }

        if (maxNewTokens != null) {
            if (hasOne) {
                parameters.append(",");
            }
            parameters.append(Strings.format("\"max_tokens\":%d", maxNewTokens));
        }

        return expected + parameters;
    }
}
