/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaifoundry.completion;

import org.elasticsearch.core.Nullable;
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


public class AzureAiFoundryChatCompletionModel extends AzureAiFoundryModel {

    public static AzureAiFoundryChatCompletionModel of(AzureAiFoundryModel model, Map<String, Object> taskSettings) {
        var modelAsCompletionModel = (AzureAiFoundryChatCompletionModel) model;

        if (taskSettings == null || taskSettings.isEmpty()) {
            return modelAsCompletionModel;
        }

        var requestTaskSettings = AzureAiFoundryChatCompletionRequestTaskSettings.fromMap(taskSettings);
        var taskSettingToUse = AzureAiFoundryChatCompletionTaskSettings.of(modelAsCompletionModel.getTaskSettings(), requestTaskSettings);

        return new AzureAiFoundryChatCompletionModel(modelAsCompletionModel, taskSettingToUse);
    }

    public AzureAiFoundryChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AzureAiFoundryChatCompletionServiceSettings serviceSettings,
        AzureAiFoundryChatCompletionTaskSettings taskSettings,
        DefaultSecretSettings secrets
    ) {
        super(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secrets));
    }

    public AzureAiFoundryChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            AzureAiFoundryChatCompletionServiceSettings.fromMap(serviceSettings, context),
            AzureAiFoundryChatCompletionTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public AzureAiFoundryChatCompletionModel(AzureAiFoundryChatCompletionModel model, AzureAiFoundryChatCompletionTaskSettings taskSettings) {
        super(model, taskSettings, model.getServiceSettings().rateLimitSettings());
    }

    @Override
    public AzureAiFoundryChatCompletionServiceSettings getServiceSettings() {
        return (AzureAiFoundryChatCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public AzureAiFoundryChatCompletionTaskSettings getTaskSettings() {
        return (AzureAiFoundryChatCompletionTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return super.getSecretSettings();
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
