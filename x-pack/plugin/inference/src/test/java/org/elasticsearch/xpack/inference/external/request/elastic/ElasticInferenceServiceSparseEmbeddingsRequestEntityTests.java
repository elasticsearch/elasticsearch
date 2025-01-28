/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.elastic;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class ElasticInferenceServiceSparseEmbeddingsRequestEntityTests extends ESTestCase {

    public void testToXContent_SingleInput() throws IOException {
        var entity = new ElasticInferenceServiceSparseEmbeddingsRequestEntity(List.of("abc"), "my-model-id");
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "input": ["abc"],
                "model_id": "my-model-id"
            }"""));
    }

    public void testToXContent_MultipleInputs() throws IOException {
        var entity = new ElasticInferenceServiceSparseEmbeddingsRequestEntity(List.of("abc", "def"), "my-model-id");
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "input": [
                    "abc",
                    "def"
                ],
                "model_id": "my-model-id"
            }
            """));
    }

    private String xContentEntityToString(ElasticInferenceServiceSparseEmbeddingsRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        return Strings.toString(builder);
    }
}
