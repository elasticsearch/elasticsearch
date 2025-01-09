/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioDeploymentType;

import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import static org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsTaskSettingsTests.getTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AzureAiStudioEmbeddingsModelTests extends ESTestCase {

    private static final AzureAiStudioDeploymentType DEFAULT_DEPLOYMENT_TYPE = AzureAiStudioDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE;
    private static final String DEFAULT_MODEL = "test-model";

    public void testOverrideWith_OverridesUser() {
        var model = createModel(
            "id",
            "http://testtarget.local",
            DEFAULT_DEPLOYMENT_TYPE,
            DEFAULT_MODEL,
            "apikey",
            null,
            false,
            null,
            null,
            null,
            null
        );

        var requestTaskSettingsMap = getTaskSettingsMap(null);
        var overriddenModel = AzureAiStudioEmbeddingsModel.of(model, requestTaskSettingsMap);

        assertThat(
            overriddenModel,
            is(
                createModel(
                    "id",
                    "http://testtarget.local",
                    DEFAULT_DEPLOYMENT_TYPE,
                    DEFAULT_MODEL,
                    "apikey",
                    null,
                    false,
                    null,
                    null,
                    null,
                    null
                )
            )
        );
    }

    public void testOverrideWith_OverridesWithoutValues() {
        var model = createModel(
            "id",
            "http://testtarget.local",
            DEFAULT_DEPLOYMENT_TYPE,
            DEFAULT_MODEL,
            "apikey",
            null,
            false,
            null,
            null,
            null,
            null
        );

        var requestTaskSettingsMap = getTaskSettingsMap(null);
        var overriddenModel = AzureAiStudioEmbeddingsModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, sameInstance(overriddenModel));
    }

    public static AzureAiStudioEmbeddingsModel createModel(
        String inferenceId,
        String target,
        AzureAiStudioDeploymentType deploymentType,
        @Nullable String model,
        String apiKey
    ) {
        return createModel(inferenceId, target, deploymentType, model, apiKey, null, false, null, null, null, null);
    }

    public static AzureAiStudioEmbeddingsModel createModel(
        String inferenceId,
        String target,
        AzureAiStudioDeploymentType deploymentType,
        @Nullable String model,
        ChunkingSettings chunkingSettings,
        String apiKey
    ) {
        return createModel(inferenceId, target, deploymentType, model, chunkingSettings, apiKey, null, false, null, null, null, null);
    }

    public static AzureAiStudioEmbeddingsModel createModel(
        String inferenceId,
        String target,
        AzureAiStudioDeploymentType deploymentType,
        @Nullable String model,
        ChunkingSettings chunkingSettings,
        String apiKey,
        @Nullable Integer dimensions,
        boolean dimensionsSetByUser,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarity,
        @Nullable String user,
        RateLimitSettings rateLimitSettings
    ) {
        return new AzureAiStudioEmbeddingsModel(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            "azureaistudio",
            new AzureAiStudioEmbeddingsServiceSettings(
                target,
                deploymentType,
                model,
                dimensions,
                dimensionsSetByUser,
                maxTokens,
                similarity,
                rateLimitSettings
            ),
            new AzureAiStudioEmbeddingsTaskSettings(user),
            chunkingSettings,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static AzureAiStudioEmbeddingsModel createModel(
        String inferenceId,
        String target,
        AzureAiStudioDeploymentType deploymentType,
        @Nullable String model,
        String apiKey,
        @Nullable Integer dimensions,
        boolean dimensionsSetByUser,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarity,
        @Nullable String user,
        RateLimitSettings rateLimitSettings
    ) {
        return new AzureAiStudioEmbeddingsModel(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            "azureaistudio",
            new AzureAiStudioEmbeddingsServiceSettings(
                target,
                deploymentType,
                model,
                dimensions,
                dimensionsSetByUser,
                maxTokens,
                similarity,
                rateLimitSettings
            ),
            new AzureAiStudioEmbeddingsTaskSettings(user),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
