/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaifoundry;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.azureaifoundry.AzureAiFoundryActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for Azure AI Foundry models. There are some common properties across the task types
 * including:
 * - target:
 * - uri:
 * - provider:
 * - endpointType:
 */
public abstract class AzureAiFoundryModel extends Model {
    protected String target;
    protected URI uri;
    protected AzureAiFoundryDeploymentType deploymentType;
    protected String model;
    protected RateLimitSettings rateLimitSettings;

    public AzureAiFoundryModel(AzureAiFoundryModel model, TaskSettings taskSettings, RateLimitSettings rateLimitSettings) {
        super(model, taskSettings);
        this.rateLimitSettings = Objects.requireNonNull(rateLimitSettings);
        setPropertiesFromServiceSettings((AzureAiFoundryServiceSettings) model.getServiceSettings());
    }

    public AzureAiFoundryModel(AzureAiFoundryModel model, AzureAiFoundryServiceSettings serviceSettings) {
        super(model, serviceSettings);
        setPropertiesFromServiceSettings(serviceSettings);
    }

    protected AzureAiFoundryModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(modelConfigurations, modelSecrets);
        setPropertiesFromServiceSettings((AzureAiFoundryServiceSettings) modelConfigurations.getServiceSettings());
    }

    private void setPropertiesFromServiceSettings(AzureAiFoundryServiceSettings serviceSettings) {
        this.target = serviceSettings.target;
        this.deploymentType = serviceSettings.deploymentType;
        if (serviceSettings.deploymentType == AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE) {
            this.model = serviceSettings.model;
        } else {
            this.model = null;
        }
        this.rateLimitSettings = serviceSettings.rateLimitSettings();
        try {
            this.uri = getEndpointUri();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract URI getEndpointUri() throws URISyntaxException;

    public String target() {
        return this.target;
    }

    public AzureAiFoundryDeploymentType deploymentType() {
        return this.deploymentType;
    }

    public String model() {
        return this.model;
    }

    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
    }

    public URI uri() {
        return this.uri;
    }

    // Needed for testing only
    public void setURI(String newUri) {
        try {
            this.uri = new URI(newUri);
        } catch (URISyntaxException e) {
            // swallow any error
        }
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    public abstract ExecutableAction accept(AzureAiFoundryActionVisitor creator, Map<String, Object> taskSettings);
}
