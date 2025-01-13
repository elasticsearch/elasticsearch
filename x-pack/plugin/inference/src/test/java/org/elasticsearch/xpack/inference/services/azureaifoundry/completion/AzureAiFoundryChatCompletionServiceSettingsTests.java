/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaifoundry.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryDeploymentType;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryConstants.DEPLOYMENT_TYPE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryConstants.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryConstants.TARGET_FIELD;
import static org.hamcrest.Matchers.is;

public class AzureAiFoundryChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    AzureAiFoundryChatCompletionServiceSettings> {
    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var target = "http://sometarget.local";
        var deploymentType = AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE.toString();
        var model = "test-model";

        var serviceSettings = AzureAiFoundryChatCompletionServiceSettings.fromMap(
            createRequestSettingsMap(target, deploymentType, model),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(new AzureAiFoundryChatCompletionServiceSettings(
                target, AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE, model, null))
        );
    }

    public void testFromMap_RequestWithRateLimit_CreatesSettingsCorrectly() {
        var target = "http://sometarget.local";
        var deploymentType = AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE.toString();
        var model = "test-model";

        var settingsMap = createRequestSettingsMap(target, deploymentType, model);
        settingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 3)));

        var serviceSettings = AzureAiFoundryChatCompletionServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(
                new AzureAiFoundryChatCompletionServiceSettings(
                    target,
                    AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
                    model,
                    new RateLimitSettings(3)
                )
            )
        );
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        var target = "http://sometarget.local";
        var deploymentType = AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE.toString();
        var model = "test-model";

        var serviceSettings = AzureAiFoundryChatCompletionServiceSettings.fromMap(
            createRequestSettingsMap(target, deploymentType, model),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(new AzureAiFoundryChatCompletionServiceSettings(
                target, AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE, model, null))
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var settings = new AzureAiFoundryChatCompletionServiceSettings(
            "target_value",
            AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            "test-model",
            new RateLimitSettings(3)
        );
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"target":"target_value","deployment_type":"azure_ai_model_inference_service","model":"test-model",""" + """
            "rate_limit":{"requests_per_minute":3}}"""));
    }

    public void testToFilteredXContent_WritesAllValues() throws IOException {
        var settings = new AzureAiFoundryChatCompletionServiceSettings(
            "target_value",
            AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE,
            "test-model",
            new RateLimitSettings(3)
        );
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = settings.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"target":"target_value","deployment_type":"azure_ai_model_inference_service","model":"test-model",""" + """
            "rate_limit":{"requests_per_minute":3}}"""));
    }

    public static HashMap<String, Object> createRequestSettingsMap(String target, String deploymentType, String model) {
        return new HashMap<>(Map.of(TARGET_FIELD, target, DEPLOYMENT_TYPE_FIELD, deploymentType, MODEL_FIELD, model));
    }

    @Override
    protected Writeable.Reader<AzureAiFoundryChatCompletionServiceSettings> instanceReader() {
        return AzureAiFoundryChatCompletionServiceSettings::new;
    }

    @Override
    protected AzureAiFoundryChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureAiFoundryChatCompletionServiceSettings mutateInstance(AzureAiFoundryChatCompletionServiceSettings instance)
        throws IOException {
        return randomValueOtherThan(instance, AzureAiFoundryChatCompletionServiceSettingsTests::createRandom);
    }

    @Override
    protected AzureAiFoundryChatCompletionServiceSettings mutateInstanceForVersion(
        AzureAiFoundryChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static AzureAiFoundryChatCompletionServiceSettings createRandom() {
        return new AzureAiFoundryChatCompletionServiceSettings(
            randomAlphaOfLength(10),
            randomFrom(AzureAiFoundryDeploymentType.values()),
            randomAlphaOfLength(15),
            RateLimitSettingsTests.createRandom()
        );
    }

}
