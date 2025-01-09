/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureaistudio;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioDeploymentType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.DIMENSIONS_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.INPUT_FIELD;

public record AzureAiStudioEmbeddingsRequestEntity(
    AzureAiStudioDeploymentType deploymentType,
    @Nullable String model,
    List<String> input,
    @Nullable Integer dimensions,
    boolean dimensionsSetByUser
) implements ToXContentObject {

    public AzureAiStudioEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(deploymentType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (deploymentType == AzureAiStudioDeploymentType.AZURE_AI_MODEL_INFERENCE_SERVICE) {
            builder.field(MODEL_FIELD, model);
        }

        builder.field(INPUT_FIELD, input);

        if (dimensionsSetByUser && dimensions != null) {
            builder.field(DIMENSIONS_FIELD, dimensions);
        }

        builder.endObject();

        return builder;
    }
}
