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
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.azureaifoundry.embeddings.AzureAiFoundryEmbeddingsModel;

import java.nio.charset.StandardCharsets;

public class AzureAiFoundryEmbeddingsRequest extends AzureAiFoundryRequest {

    private final AzureAiFoundryEmbeddingsModel embeddingsModel;
    private final Truncator.TruncationResult truncationResult;
    private final Truncator truncator;

    public AzureAiFoundryEmbeddingsRequest(Truncator truncator, Truncator.TruncationResult input, AzureAiFoundryEmbeddingsModel model) {
        super(model);
        this.embeddingsModel = model;
        this.truncator = truncator;
        this.truncationResult = input;
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(this.uri);

        var dimensions = embeddingsModel.getServiceSettings().dimensions();
        var dimensionsSetByUser = embeddingsModel.getServiceSettings().dimensionsSetByUser();
        var deploymentType = embeddingsModel.getServiceSettings().deploymentType();
        var model = embeddingsModel.getServiceSettings().model();

        ByteArrayEntity byteEntity = new ByteArrayEntity(Strings.toString(
            new AzureAiFoundryEmbeddingsRequestEntity(deploymentType,
                model,
                truncationResult.input(),
                dimensions,
                dimensionsSetByUser))
            .getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        setAuthHeader(httpPost, embeddingsModel);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public Request truncate() {
        var truncatedInput = truncator.truncate(truncationResult.input());
        return new AzureAiFoundryEmbeddingsRequest(truncator, truncatedInput, embeddingsModel);
    }

    @Override
    public boolean[] getTruncationInfo() {
        return truncationResult.truncated().clone();
    }
}
