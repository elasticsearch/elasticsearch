/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaifoundry.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryConstants;
import org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryDeploymentType;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureAiFoundryEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    AzureAiFoundryEmbeddingsServiceSettings> {

    private static final String DEFAULT_MODEL = "test-model";

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var target = "http://sometarget.local";
        var deploymentType = AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var serviceSettings = AzureAiFoundryEmbeddingsServiceSettings.fromMap(
            createRequestSettingsMap(target, deploymentType, DEFAULT_MODEL, dims, null, maxInputTokens, SimilarityMeasure.COSINE),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new AzureAiFoundryEmbeddingsServiceSettings(
                    target,
                    AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
                    DEFAULT_MODEL,
                    dims,
                    true,
                    maxInputTokens,
                    SimilarityMeasure.COSINE,
                    null
                )
            )
        );
    }

    public void testFromMap_RequestWithRateLimit_CreatesSettingsCorrectly() {
        var target = "http://sometarget.local";
        var deploymentType = AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var settingsMap = createRequestSettingsMap(
            target, deploymentType, DEFAULT_MODEL, dims, null, maxInputTokens, SimilarityMeasure.COSINE);
        settingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 3)));

        var serviceSettings = AzureAiFoundryEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(
                new AzureAiFoundryEmbeddingsServiceSettings(
                    target,
                    AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
                    DEFAULT_MODEL,
                    dims,
                    true,
                    maxInputTokens,
                    SimilarityMeasure.COSINE,
                    new RateLimitSettings(3)
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_IsFalse_WhenDimensionsAreNotPresent() {
        var target = "http://sometarget.local";
        var deploymentType = AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE.toString();
        var maxInputTokens = 512;
        var settingsMap = createRequestSettingsMap(
            target, deploymentType, DEFAULT_MODEL, null, null, maxInputTokens, SimilarityMeasure.COSINE);
        var serviceSettings = AzureAiFoundryEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(
                new AzureAiFoundryEmbeddingsServiceSettings(
                    target,
                    AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
                    DEFAULT_MODEL,
                    null,
                    false,
                    maxInputTokens,
                    SimilarityMeasure.COSINE,
                    null
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_ShouldThrowWhenPresent() {
        var target = "http://sometarget.local";
        var deploymentType = AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE.toString();
        var maxInputTokens = 512;

        var settingsMap = createRequestSettingsMap(
            target, deploymentType, DEFAULT_MODEL, null, true, maxInputTokens, SimilarityMeasure.COSINE);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiFoundryEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] does not allow the setting [%s];",
                    AzureAiFoundryConstants.DIMENSIONS_SET_BY_USER
                )
            )
        );
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        var target = "http://sometarget.local";
        var deploymentType = AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE.toString();
        var dims = 1536;
        var maxInputTokens = 512;

        var settingsMap = createRequestSettingsMap(
            target, deploymentType, DEFAULT_MODEL, dims, false, maxInputTokens, SimilarityMeasure.COSINE);
        var serviceSettings = AzureAiFoundryEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new AzureAiFoundryEmbeddingsServiceSettings(
                    target,
                    AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
                    DEFAULT_MODEL,
                    dims,
                    false,
                    maxInputTokens,
                    SimilarityMeasure.COSINE,
                    null
                )
            )
        );
    }

    public void testFromMap_ThrowsException_WhenDimensionsAreZero() {
        var target = "http://sometarget.local";
        var deploymentType = AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE.toString();
        var dimensions = 0;

        var settingsMap = createRequestSettingsMap(
            target, deploymentType, DEFAULT_MODEL, dimensions, true, null, SimilarityMeasure.COSINE);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiFoundryEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value [0]. [dimensions] must be a positive integer;")
        );
    }

    public void testFromMap_ThrowsException_WhenDimensionsAreNegative() {
        var target = "http://sometarget.local";
        var deploymentType = AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE.toString();
        var dimensions = randomNegativeInt();

        var settingsMap = createRequestSettingsMap(
            target, deploymentType, DEFAULT_MODEL, dimensions, true, null, SimilarityMeasure.COSINE);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiFoundryEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%d]. [dimensions] must be a positive integer;",
                    dimensions
                )
            )
        );
    }

    public void testFromMap_ThrowsException_WhenMaxInputTokensAreZero() {
        var target = "http://sometarget.local";
        var deploymentType = AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE.toString();
        var maxInputTokens = 0;

        var settingsMap = createRequestSettingsMap(
            target, deploymentType, DEFAULT_MODEL, null, true, maxInputTokens, SimilarityMeasure.COSINE);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiFoundryEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value [0]. [max_input_tokens] must be a positive integer;")
        );
    }

    public void testFromMap_ThrowsException_WhenMaxInputTokensAreNegative() {
        var target = "http://sometarget.local";
        var deploymentType = AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE.toString();
        var maxInputTokens = randomNegativeInt();

        var settingsMap = createRequestSettingsMap(
            target, deploymentType, DEFAULT_MODEL, null, true, maxInputTokens, SimilarityMeasure.COSINE);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiFoundryEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%d]. [max_input_tokens] must be a positive integer;",
                    maxInputTokens
                )
            )
        );
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenDimensionsIsNull() {
        var target = "http://sometarget.local";
        var deploymentType = AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE.toString();

        var settingsMap = createRequestSettingsMap(target, deploymentType, DEFAULT_MODEL, null, true, null, null);
        var serviceSettings = AzureAiFoundryEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new AzureAiFoundryEmbeddingsServiceSettings(
                    target,
                    AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
                    DEFAULT_MODEL,
                    null,
                    true,
                    null,
                    null,
                    null
                )
            )
        );
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenSimilarityIsPresent() {
        var target = "http://sometarget.local";
        var deploymentType = AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE.toString();

        var settingsMap = createRequestSettingsMap(target, deploymentType, DEFAULT_MODEL, null, true, null, SimilarityMeasure.DOT_PRODUCT);
        var serviceSettings = AzureAiFoundryEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new AzureAiFoundryEmbeddingsServiceSettings(
                    target,
                    AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
                    DEFAULT_MODEL,
                    null,
                    true,
                    null,
                    SimilarityMeasure.DOT_PRODUCT,
                    null
                )
            )
        );
    }

    public void testFromMap_PersistentContext_ThrowsException_WhenDimensionsSetByUserIsNull() {
        var target = "http://sometarget.local";
        var deploymentType = AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE.toString();

        var settingsMap = createRequestSettingsMap(
            target, deploymentType, DEFAULT_MODEL, 1, null, null, null);

        var exception = expectThrows(
            ValidationException.class,
            () -> AzureAiFoundryEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT)
        );

        assertThat(
            exception.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [dimensions_set_by_user];")
        );
    }

    public void testToXContent_WritesDimensionsSetByUserTrue() throws IOException {
        var entity = new AzureAiFoundryEmbeddingsServiceSettings(
            "target_value",
            AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            null,
            true,
            null,
            null,
            new RateLimitSettings(2)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"target":"target_value","deployment_type":"azure_ai_model_inference_service","model":"test-model",""" + """
            "rate_limit":{"requests_per_minute":2},"dimensions_set_by_user":true}"""));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new AzureAiFoundryEmbeddingsServiceSettings(
            "target_value",
            AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            1024,
            false,
            512,
            null,
            new RateLimitSettings(3)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"target":"target_value","deployment_type":"azure_ai_model_inference_service","model":"test-model",""" + """
            "rate_limit":{"requests_per_minute":3},"dimensions":1024,"max_input_tokens":512,"dimensions_set_by_user":false}"""));
    }

    public void testToFilteredXContent_WritesAllValues_ExceptDimensionsSetByUser() throws IOException {
        var entity = new AzureAiFoundryEmbeddingsServiceSettings(
            "target_value",
            AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            DEFAULT_MODEL,
            1024,
            false,
            512,
            null,
            new RateLimitSettings(3)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"target":"target_value","deployment_type":"azure_ai_model_inference_service","model":"test-model",""" + """
            "rate_limit":{"requests_per_minute":3},"dimensions":1024,"max_input_tokens":512}"""));
    }

    public static HashMap<String, Object> createRequestSettingsMap(
        String target,
        String deploymentType,
        String model,
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarityMeasure
    ) {
        var map = new HashMap<String, Object>(
            Map.of(
                AzureAiFoundryConstants.TARGET_FIELD,
                target,
                AzureAiFoundryConstants.DEPLOYMENT_TYPE_FIELD,
                deploymentType,
                AzureAiFoundryConstants.MODEL_FIELD,
                model
            )
        );

        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }

        if (dimensionsSetByUser != null) {
            map.put(AzureAiFoundryConstants.DIMENSIONS_SET_BY_USER, dimensionsSetByUser.equals(Boolean.TRUE));
        }

        if (maxTokens != null) {
            map.put(ServiceFields.MAX_INPUT_TOKENS, maxTokens);
        }

        if (similarityMeasure != null) {
            map.put(SIMILARITY, similarityMeasure.toString());
        }

        return map;
    }

    @Override
    protected Writeable.Reader<AzureAiFoundryEmbeddingsServiceSettings> instanceReader() {
        return AzureAiFoundryEmbeddingsServiceSettings::new;
    }

    @Override
    protected AzureAiFoundryEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureAiFoundryEmbeddingsServiceSettings mutateInstance(AzureAiFoundryEmbeddingsServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, AzureAiFoundryEmbeddingsServiceSettingsTests::createRandom);
    }

    @Override
    protected AzureAiFoundryEmbeddingsServiceSettings mutateInstanceForVersion(
        AzureAiFoundryEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static AzureAiFoundryEmbeddingsServiceSettings createRandom() {
        return new AzureAiFoundryEmbeddingsServiceSettings(
            randomAlphaOfLength(10),
            randomFrom(AzureAiFoundryDeploymentType.values()),
            randomAlphaOfLength(10),
            randomFrom(new Integer[] { null, randomNonNegativeInt() }),
            randomBoolean(),
            randomFrom(new Integer[] { null, randomNonNegativeInt() }),
            randomFrom(new SimilarityMeasure[] { null, randomFrom(SimilarityMeasure.values()) }),
            RateLimitSettingsTests.createRandom()
        );
    }
}
