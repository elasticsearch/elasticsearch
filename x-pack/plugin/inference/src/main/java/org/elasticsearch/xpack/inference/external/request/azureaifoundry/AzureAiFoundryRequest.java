/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureaifoundry;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryDeploymentType;
import org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryModel;
import org.elasticsearch.xpack.inference.external.request.RequestUtils;

import java.net.URI;

import static org.elasticsearch.xpack.inference.external.request.azureaifoundry.AzureAiFoundryRequestFields.API_KEY_HEADER;

public abstract class AzureAiFoundryRequest implements Request {

    protected final URI uri;
    protected final String inferenceEntityId;

    protected AzureAiFoundryRequest(AzureAiFoundryModel model) {
        this.uri = model.uri();
        this.inferenceEntityId = model.getInferenceEntityId();
    }

    protected void setAuthHeader(HttpEntityEnclosingRequestBase request, AzureAiFoundryModel model) {
        var apiKey = model.getSecretSettings().apiKey();
        if (model.deploymentType() == AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE) {
            request.setHeader(API_KEY_HEADER, apiKey.toString());
        } else if (model.deploymentType() == AzureAiFoundryDeploymentType.SERVERLESS_API) {
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
