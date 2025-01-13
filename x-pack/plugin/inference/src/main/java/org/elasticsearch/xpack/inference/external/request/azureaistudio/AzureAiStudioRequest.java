/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureaistudio;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioDeploymentType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioModel;
import org.elasticsearch.xpack.inference.external.request.RequestUtils;

import java.net.URI;

import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.API_KEY_HEADER;

public abstract class AzureAiStudioRequest implements Request {

    protected final URI uri;
    protected final String inferenceEntityId;

    protected AzureAiStudioRequest(AzureAiStudioModel model) {
        this.uri = model.uri();
        this.inferenceEntityId = model.getInferenceEntityId();
    }

    protected void setAuthHeader(HttpEntityEnclosingRequestBase request, AzureAiStudioModel model) {
        var apiKey = model.getSecretSettings().apiKey();
        if (model.deploymentType() == AzureAiStudioDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE) {
            request.setHeader(API_KEY_HEADER, apiKey.toString());
        } else if (model.deploymentType() == AzureAiStudioDeploymentType.SERVERLESS_API) {
            request.setHeader(RequestUtils.createAuthBearerHeader(apiKey));
        } else {
            // default to api-key header
            request.setHeader(API_KEY_HEADER, apiKey.toString());
        }
    }

    @Override
    public URI getURI() {
        return this.uri;
    }

    @Override
    public String getInferenceEntityId() {
        return this.inferenceEntityId;
    }

}
