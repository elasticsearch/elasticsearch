/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaifoundry.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.azureaifoundry.AzureAiFoundryActionVisitor;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class AzureAiFoundryEmbeddingsModel extends AzureAiFoundryModel {

    public static AzureAiFoundryEmbeddingsModel of(AzureAiFoundryEmbeddingsModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        var requestTaskSettings = AzureAiFoundryEmbeddingsRequestTaskSettings.fromMap(taskSettings);
        var taskSettingToUse = AzureAiFoundryEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings);

        return new AzureAiFoundryEmbeddingsModel(model, taskSettingToUse);
    }

    public AzureAiFoundryEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AzureAiFoundryEmbeddingsServiceSettings serviceSettings,
        AzureAiFoundryEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secrets)
        );
    }

    public AzureAiFoundryEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            AzureAiFoundryEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            AzureAiFoundryEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    private AzureAiFoundryEmbeddingsModel(AzureAiFoundryEmbeddingsModel model, AzureAiFoundryEmbeddingsTaskSettings taskSettings) {
        super(model, taskSettings, model.getServiceSettings().rateLimitSettings());
    }

    public AzureAiFoundryEmbeddingsModel(AzureAiFoundryEmbeddingsModel model, AzureAiFoundryEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public AzureAiFoundryEmbeddingsServiceSettings getServiceSettings() {
        return (AzureAiFoundryEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public AzureAiFoundryEmbeddingsTaskSettings getTaskSettings() {
        return (AzureAiFoundryEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    protected URI getEndpointUri() throws URISyntaxException {
        return new URI(this.target);
    }

    @Override
    public ExecutableAction accept(AzureAiFoundryActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
