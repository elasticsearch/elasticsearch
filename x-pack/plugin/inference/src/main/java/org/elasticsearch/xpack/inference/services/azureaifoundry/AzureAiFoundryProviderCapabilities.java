/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaifoundry;

import org.elasticsearch.inference.TaskType;

import java.util.List;

public final class AzureAiFoundryProviderCapabilities {

    // these providers have embeddings inference
    public static final List<AzureAiFoundryProvider> embeddingProviders = List.of(
        AzureAiFoundryProvider.OPENAI,
        AzureAiFoundryProvider.COHERE
    );

    // these providers have chat completion inference (all providers at the moment)
    public static final List<AzureAiFoundryProvider> chatCompletionProviders = List.of(AzureAiFoundryProvider.values());

    // these providers allow token ("pay as you go") embeddings endpoints
    public static final List<AzureAiFoundryProvider> tokenEmbeddingsProviders = List.of(
        AzureAiFoundryProvider.OPENAI,
        AzureAiFoundryProvider.COHERE
    );

    // these providers allow realtime embeddings endpoints (none at the moment)
    public static final List<AzureAiFoundryProvider> realtimeEmbeddingsProviders = List.of();

    // these providers allow token ("pay as you go") chat completion endpoints
    public static final List<AzureAiFoundryProvider> tokenChatCompletionProviders = List.of(
        AzureAiFoundryProvider.OPENAI,
        AzureAiFoundryProvider.META,
        AzureAiFoundryProvider.COHERE
    );

    // these providers allow realtime chat completion endpoints
    public static final List<AzureAiFoundryProvider> realtimeChatCompletionProviders = List.of(
        AzureAiFoundryProvider.MISTRAL,
        AzureAiFoundryProvider.META,
        AzureAiFoundryProvider.MICROSOFT_PHI,
        AzureAiFoundryProvider.DATABRICKS
    );

    public static boolean providerAllowsTaskType(AzureAiFoundryProvider provider, TaskType taskType) {
        switch (taskType) {
            case COMPLETION -> {
                return chatCompletionProviders.contains(provider);
            }
            case TEXT_EMBEDDING -> {
                return embeddingProviders.contains(provider);
            }
            default -> {
                return false;
            }
        }
    }

    public static boolean providerAllowsEndpointTypeForTask(
        AzureAiFoundryProvider provider,
        TaskType taskType,
        AzureAiFoundryEndpointType endpointType
    ) {
        switch (taskType) {
            case COMPLETION -> {
                return (endpointType == AzureAiFoundryEndpointType.TOKEN)
                    ? tokenChatCompletionProviders.contains(provider)
                    : realtimeChatCompletionProviders.contains(provider);
            }
            case TEXT_EMBEDDING -> {
                return (endpointType == AzureAiFoundryEndpointType.TOKEN)
                    ? tokenEmbeddingsProviders.contains(provider)
                    : realtimeEmbeddingsProviders.contains(provider);
            }
            default -> {
                return false;
            }
        }
    }

}
