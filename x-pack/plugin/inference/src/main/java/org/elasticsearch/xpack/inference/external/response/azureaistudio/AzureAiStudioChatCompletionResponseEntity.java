/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.azureaistudio;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioChatCompletionRequest;
import org.elasticsearch.xpack.inference.external.response.BaseResponseEntity;
import org.elasticsearch.xpack.inference.external.response.openai.OpenAiChatCompletionResponseEntity;

import java.io.IOException;

public class AzureAiStudioChatCompletionResponseEntity extends BaseResponseEntity {

    @Override
    protected InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        if (request instanceof AzureAiStudioChatCompletionRequest asChatCompletionRequest) {

            // we can use the OpenAI chat completion type as it is the same as Azure AI Studio's format
            return OpenAiChatCompletionResponseEntity.fromResponse(request, response);
        }

        return null;
    }
}
