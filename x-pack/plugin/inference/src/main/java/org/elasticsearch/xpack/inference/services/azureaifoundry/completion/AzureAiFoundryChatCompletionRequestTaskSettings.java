/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaifoundry.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryConstants;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalDoubleInRange;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryConstants.DO_SAMPLE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryConstants.MAX_TOKENS_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryConstants.TEMPERATURE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryConstants.TOP_P_FIELD;

public record AzureAiFoundryChatCompletionRequestTaskSettings(
    @Nullable Double temperature,
    @Nullable Double topP,
    @Nullable Boolean doSample,
    @Nullable Integer maxTokens
) {

    public static final AzureAiFoundryChatCompletionRequestTaskSettings EMPTY_SETTINGS = new AzureAiFoundryChatCompletionRequestTaskSettings(
        null,
        null,
        null,
        null
    );

    /**
     * Extracts the task settings from a map. All settings are considered optional and the absence of a setting
     * does not throw an error.
     *
     * @param map the settings received from a request
     * @return a {@link AzureAiFoundryChatCompletionRequestTaskSettings}
     */
    public static AzureAiFoundryChatCompletionRequestTaskSettings fromMap(Map<String, Object> map) {
        if (map.isEmpty()) {
            return AzureAiFoundryChatCompletionRequestTaskSettings.EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        var temperature = extractOptionalDoubleInRange(
            map,
            TEMPERATURE_FIELD,
            AzureAiFoundryConstants.MIN_TEMPERATURE_TOP_P,
            AzureAiFoundryConstants.MAX_TEMPERATURE_TOP_P,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        var topP = extractOptionalDoubleInRange(
            map,
            TOP_P_FIELD,
            AzureAiFoundryConstants.MIN_TEMPERATURE_TOP_P,
            AzureAiFoundryConstants.MAX_TEMPERATURE_TOP_P,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        Boolean doSample = extractOptionalBoolean(map, DO_SAMPLE_FIELD, validationException);
        Integer maxTokens = extractOptionalPositiveInteger(
            map,
            MAX_TOKENS_FIELD,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureAiFoundryChatCompletionRequestTaskSettings(temperature, topP, doSample, maxTokens);
    }
}
