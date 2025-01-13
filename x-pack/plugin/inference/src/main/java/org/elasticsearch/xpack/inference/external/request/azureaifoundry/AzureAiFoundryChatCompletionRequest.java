/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureaifoundry;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.azureaifoundry.completion.AzureAiFoundryChatCompletionModel;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class AzureAiFoundryChatCompletionRequest extends AzureAiFoundryRequest {
    private final List<String> input;
    private final AzureAiFoundryChatCompletionModel completionModel;
    private final boolean stream;

    public AzureAiFoundryChatCompletionRequest(AzureAiFoundryChatCompletionModel model, List<String> input, boolean stream) {
        super(model);
        this.input = Objects.requireNonNull(input);
        this.completionModel = Objects.requireNonNull(model);
        this.stream = stream;
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(this.uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(Strings.toString(createRequestEntity()).getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        setAuthHeader(httpPost, completionModel);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public Request truncate() {
        // no truncation
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // no truncation
        return null;
    }

    @Override
    public boolean isStreaming() {
        return stream;
    }

    private AzureAiFoundryChatCompletionRequestEntity createRequestEntity() {
        var taskSettings = completionModel.getTaskSettings();
        var serviceSettings = completionModel.getServiceSettings();
        return new AzureAiFoundryChatCompletionRequestEntity(
            input,
            serviceSettings.deploymentType(),
            serviceSettings.model(),
            taskSettings.temperature(),
            taskSettings.topP(),
            taskSettings.doSample(),
            taskSettings.maxTokens(),
            isStreaming()
        );
    }

}
