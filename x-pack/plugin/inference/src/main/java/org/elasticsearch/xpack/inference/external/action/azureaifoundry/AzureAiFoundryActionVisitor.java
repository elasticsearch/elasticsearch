/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.azureaifoundry;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.azureaifoundry.completion.AzureAiFoundryChatCompletionModel;
import org.elasticsearch.xpack.inference.services.azureaifoundry.embeddings.AzureAiFoundryEmbeddingsModel;

import java.util.Map;

public interface AzureAiFoundryActionVisitor {
    ExecutableAction create(AzureAiFoundryEmbeddingsModel embeddingsModel, Map<String, Object> taskSettings);

    ExecutableAction create(AzureAiFoundryChatCompletionModel completionModel, Map<String, Object> taskSettings);
}
