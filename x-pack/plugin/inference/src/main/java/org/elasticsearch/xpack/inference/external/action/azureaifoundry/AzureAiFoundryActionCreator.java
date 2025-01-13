/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.azureaifoundry;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.AzureAiFoundryChatCompletionRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.AzureAiFoundryEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.azureaifoundry.completion.AzureAiFoundryChatCompletionModel;
import org.elasticsearch.xpack.inference.services.azureaifoundry.embeddings.AzureAiFoundryEmbeddingsModel;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

public class AzureAiFoundryActionCreator implements AzureAiFoundryActionVisitor {
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public AzureAiFoundryActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(AzureAiFoundryChatCompletionModel completionModel, Map<String, Object> taskSettings) {
        var overriddenModel = AzureAiFoundryChatCompletionModel.of(completionModel, taskSettings);
        var requestManager = new AzureAiFoundryChatCompletionRequestManager(overriddenModel, serviceComponents.threadPool());
        var errorMessage = constructFailedToSendRequestMessage(completionModel.uri(), "Azure AI Foundry completion");
        return new SenderExecutableAction(sender, requestManager, errorMessage);
    }

    @Override
    public ExecutableAction create(AzureAiFoundryEmbeddingsModel embeddingsModel, Map<String, Object> taskSettings) {
        var overriddenModel = AzureAiFoundryEmbeddingsModel.of(embeddingsModel, taskSettings);
        var requestManager = new AzureAiFoundryEmbeddingsRequestManager(
            overriddenModel,
            serviceComponents.truncator(),
            serviceComponents.threadPool()
        );
        var errorMessage = constructFailedToSendRequestMessage(embeddingsModel.uri(), "Azure AI Foundry embeddings");
        return new SenderExecutableAction(sender, requestManager, errorMessage);
    }
}
