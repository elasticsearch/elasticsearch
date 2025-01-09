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
import org.elasticsearch.xpack.inference.external.response.BaseResponseEntity;
import org.elasticsearch.xpack.inference.external.response.openai.OpenAiEmbeddingsResponseEntity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class AzureAiStudioEmbeddingsResponseEntity extends BaseResponseEntity {
    @Override
    protected InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        // expected response type is the same as the Open AI Embeddings
//        var stringResponse = response.response().toString();
//        var stringResponseBody = new String(response.body(), StandardCharsets.UTF_8);
//        System.out.println(stringResponse);
//        System.out.println(stringResponseBody);
        return OpenAiEmbeddingsResponseEntity.fromResponse(request, response);
    }
}
