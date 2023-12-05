/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Map;

/** A {@link FieldMapper} for full-text fields. */
public class SemanticTextFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "semantic_text";

    private static SemanticTextFieldMapper toType(FieldMapper in) {
        return (SemanticTextFieldMapper) in;
    }

    private static Builder builder(FieldMapper in) {
        return ((SemanticTextFieldMapper) in).builder;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<String> modelId = Parameter.stringParam("model_id", false, m -> builder(m).modelId.get(), null)
            .addValidator(v -> {
                if (Strings.isEmpty(v)) {
                    throw new IllegalArgumentException("field [model_id] must be specified");
                }
            });

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        public Builder modelId(String modelId) {
            this.modelId.setValue(modelId);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { modelId, meta };
        }

        @Override
        public SemanticTextFieldMapper build(MapperBuilderContext context) {
            return new SemanticTextFieldMapper(
                name(),
                new SemanticTextFieldType(name(), modelId.getValue(), meta.getValue()),
                modelId.getValue(),
                copyTo,
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n), notInMultiFields(CONTENT_TYPE));

    public static class SemanticTextFieldType extends SimpleMappedFieldType {

        private final String modelId;

        public SemanticTextFieldType(String name, String modelId, Map<String, String> meta) {
            super(name, false, false, false, TextSearchInfo.NONE, meta);
            this.modelId = modelId;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String getInferenceModel() {
            return modelId;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("termQuery not implemented yet");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.toString(name(), context, format);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("[semantic_text] fields do not support sorting, scripting or aggregating");
        }
    }

    private final String modelId;

    private final Builder builder;

    private SemanticTextFieldMapper(String simpleName, MappedFieldType mappedFieldType, String modelId, CopyTo copyTo, Builder builder) {
        super(simpleName, mappedFieldType, MultiFields.empty(), copyTo);
        this.builder = builder;
        this.modelId = modelId;
    }

    public String getModelId() {
        return modelId;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        context.parser().textOrNull();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SemanticTextFieldType fieldType() {
        return (SemanticTextFieldType) super.fieldType();
    }
}
