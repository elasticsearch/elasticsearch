/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureaifoundry;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryDeploymentType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.azureaifoundry.AzureAiFoundryRequestFields.MESSAGES_ARRAY;
import static org.elasticsearch.xpack.inference.external.request.azureaifoundry.AzureAiFoundryRequestFields.MESSAGE_CONTENT;
import static org.elasticsearch.xpack.inference.external.request.azureaifoundry.AzureAiFoundryRequestFields.ROLE;
import static org.elasticsearch.xpack.inference.external.request.azureaifoundry.AzureAiFoundryRequestFields.STREAM;
import static org.elasticsearch.xpack.inference.external.request.azureaifoundry.AzureAiFoundryRequestFields.USER_ROLE;
import static org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryConstants.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryConstants.DO_SAMPLE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryConstants.MAX_TOKENS_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryConstants.TEMPERATURE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryConstants.TOP_P_FIELD;

public record AzureAiFoundryChatCompletionRequestEntity(
    List<String> messages,
    AzureAiFoundryDeploymentType deploymentType,
    @Nullable String model,
    @Nullable Double temperature,
    @Nullable Double topP,
    @Nullable Boolean doSample,
    @Nullable Integer maxTokens,
    boolean stream
) implements ToXContentObject {

    public AzureAiFoundryChatCompletionRequestEntity {
        Objects.requireNonNull(messages);
        Objects.requireNonNull(deploymentType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (deploymentType == AzureAiFoundryDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE) {
            builder.field(MODEL_FIELD, model);
        }

        if (stream) {
            builder.field(STREAM, true);
        }

        builder.startArray(MESSAGES_ARRAY);

        for (String message : messages) {
            addMessageContentObject(builder, message);
        }

        builder.endArray();

        addRequestParameters(builder);

        builder.endObject();

        return builder;
    }

    private void addMessageContentObject(XContentBuilder builder, String message) throws IOException {
        builder.startObject();

        builder.field(MESSAGE_CONTENT, message);
        builder.field(ROLE, USER_ROLE);

        builder.endObject();
    }

    private void addRequestParameters(XContentBuilder builder) throws IOException {
        if (temperature == null && topP == null && doSample == null && maxTokens == null) {
            return;
        }

        if (temperature != null) {
            builder.field(TEMPERATURE_FIELD, temperature);
        }

        if (topP != null) {
            builder.field(TOP_P_FIELD, topP);
        }

        if (doSample != null) {
            builder.field(DO_SAMPLE_FIELD, doSample);
        }

        if (maxTokens != null) {
            builder.field(MAX_TOKENS_FIELD, maxTokens);
        }

    }
}
