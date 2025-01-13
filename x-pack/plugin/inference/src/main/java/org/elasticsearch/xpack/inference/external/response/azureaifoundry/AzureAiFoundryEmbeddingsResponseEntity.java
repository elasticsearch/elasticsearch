/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.azureaifoundry;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.BaseResponseEntity;
import org.elasticsearch.xpack.inference.external.response.openai.OpenAiEmbeddingsResponseEntity;

import java.io.IOException;

public class AzureAiFoundryEmbeddingsResponseEntity extends BaseResponseEntity {
    @Override
    protected InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        return OpenAiEmbeddingsResponseEntity.fromResponse(request, response);
    }
}
