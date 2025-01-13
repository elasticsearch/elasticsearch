/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.DEPLOYMENT_TYPE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TARGET_FIELD;

public abstract class AzureAiStudioServiceSettings extends FilteredXContentObject implements ServiceSettings {

    protected final String target;
    protected final AzureAiStudioDeploymentType deploymentType;
    protected final String model;
    protected final RateLimitSettings rateLimitSettings;

    protected static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(240);

    protected static BaseAzureAiStudioCommonFields fromMap(
        Map<String, Object> map,
        ValidationException validationException,
        ConfigurationParseContext context
    ) {
        String target = extractRequiredString(map, TARGET_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            AzureAiStudioService.NAME,
            context
        );
        AzureAiStudioDeploymentType deploymentType = extractRequiredEnum(
            map,
            DEPLOYMENT_TYPE_FIELD,
            ModelConfigurations.SERVICE_SETTINGS,
            AzureAiStudioDeploymentType::fromString,
            EnumSet.allOf(AzureAiStudioDeploymentType.class),
            validationException
        );
        String model = extractOptionalString(
            map,
            MODEL_FIELD,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        return new BaseAzureAiStudioCommonFields(target, deploymentType, model, rateLimitSettings);
    }

    protected AzureAiStudioServiceSettings(StreamInput in) throws IOException {
        this.target = in.readString();
        this.deploymentType = in.readEnum(AzureAiStudioDeploymentType.class);
        this.model = in.readString();
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    protected AzureAiStudioServiceSettings(
        String target,
        AzureAiStudioDeploymentType deploymentType,
        String model,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.target = target;
        this.deploymentType = deploymentType;
        this.model = model;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    protected record BaseAzureAiStudioCommonFields(
        String target,
        AzureAiStudioDeploymentType deploymentType,
        @Nullable String model,
        RateLimitSettings rateLimitSettings
    ) {}

    public String target() {
        return this.target;
    }

    public AzureAiStudioDeploymentType deploymentType() {
        return this.deploymentType;
    }

    public String model() {
        return this.model;
    }

    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
    }

    @Override
    public String modelId() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(target);
        out.writeEnum(deploymentType);
        out.writeString(model);
        rateLimitSettings.writeTo(out);
    }

    protected void addXContentFields(XContentBuilder builder, Params params) throws IOException {
        this.addExposedXContentFields(builder, params);
    }

    protected void addExposedXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(TARGET_FIELD, this.target);
        builder.field(DEPLOYMENT_TYPE_FIELD, this.deploymentType);
        builder.field(MODEL_FIELD, this.model);
        rateLimitSettings.toXContent(builder, params);
    }

}
