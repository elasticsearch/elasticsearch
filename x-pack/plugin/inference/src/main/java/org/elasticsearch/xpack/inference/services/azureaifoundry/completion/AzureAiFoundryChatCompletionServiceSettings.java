/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaifoundry.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryDeploymentType;
import org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class AzureAiFoundryChatCompletionServiceSettings extends AzureAiFoundryServiceSettings {
    public static final String NAME = "azure_ai_foundry_chat_completion_service_settings";

    public static AzureAiFoundryChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        var settings = completionSettingsFromMap(map, validationException, context);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureAiFoundryChatCompletionServiceSettings(settings);
    }

    private static AzureAiFoundryCompletionCommonFields completionSettingsFromMap(
        Map<String, Object> map,
        ValidationException validationException,
        ConfigurationParseContext context
    ) {
        var baseSettings = AzureAiFoundryServiceSettings.fromMap(map, validationException, context);
        return new AzureAiFoundryCompletionCommonFields(baseSettings);
    }

    private record AzureAiFoundryCompletionCommonFields(BaseAzureAiFoundryCommonFields baseCommonFields) {}

    public AzureAiFoundryChatCompletionServiceSettings(
        String target,
        AzureAiFoundryDeploymentType deploymentType,
        @Nullable String model,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        super(target, deploymentType, model, rateLimitSettings);
    }

    public AzureAiFoundryChatCompletionServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    private AzureAiFoundryChatCompletionServiceSettings(AzureAiFoundryCompletionCommonFields fields) {
        this(
            fields.baseCommonFields.target(),
            fields.baseCommonFields.deploymentType(),
            fields.baseCommonFields.model(),
            fields.baseCommonFields.rateLimitSettings()
        );
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        super.addXContentFields(builder, params);

        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        super.addExposedXContentFields(builder, params);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureAiFoundryChatCompletionServiceSettings that = (AzureAiFoundryChatCompletionServiceSettings) o;

        return Objects.equals(target, that.target)
            && Objects.equals(deploymentType, that.deploymentType)
            && Objects.equals(model, that.model)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(target, deploymentType, model, rateLimitSettings);
    }
}
