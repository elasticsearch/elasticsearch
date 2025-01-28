/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.elastic;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record ElasticInferenceServiceSparseEmbeddingsRequestEntity(List<String> inputs, String modelId) implements ToXContentObject {

    private static final String INPUT_FIELD = "input";
    private static final String MODEL_ID_FIELD = "model_id";

    public ElasticInferenceServiceSparseEmbeddingsRequestEntity {
        Objects.requireNonNull(inputs);
        Objects.requireNonNull(modelId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(INPUT_FIELD);

        for (String input : inputs) {
            builder.value(input);
        }

        builder.endArray();
        builder.field(MODEL_ID_FIELD, modelId);
        builder.endObject();

        return builder;
    }
}
