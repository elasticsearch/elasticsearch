/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockSourceReader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.InferenceFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperMergeContext;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.queries.SparseVectorQueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.DEFAULT_ELSER_ID;

/**
 * A {@link FieldMapper} for semantic text fields.
 */
public class SemanticTextFieldMapper extends FieldMapper implements InferenceFieldMapper {
    public static final String CONTENT_TYPE = "semantic_text";
    public static final String DEFAULT_ELSER_2_INFERENCE_ID = DEFAULT_ELSER_ID;

    private final IndexSettings indexSettings;

    public static final TypeParser PARSER = new TypeParser((n, c) -> {
        if (c.getIndexSettings().getIndexVersionCreated().onOrAfter(IndexVersions.INFERENCE_METADATA_FIELDS)) {
            return new Builder(n, c::bitSetProducer, c.getIndexSettings());
        }
        return new LegacySemanticTextFieldMapper.Builder(n, c.indexVersionCreated(), c::bitSetProducer, c.getIndexSettings());
    }, List.of(notInMultiFields(CONTENT_TYPE), notFromDynamicTemplates(CONTENT_TYPE)));

    public static class Builder extends FieldMapper.Builder {
        private final IndexSettings indexSettings;

        private final Parameter<String> inferenceId = Parameter.stringParam(
            SemanticTextField.INFERENCE_ID_FIELD,
            false,
            mapper -> ((SemanticTextFieldType) mapper.fieldType()).inferenceId,
            DEFAULT_ELSER_2_INFERENCE_ID
        ).addValidator(v -> {
            if (Strings.isEmpty(v)) {
                throw new IllegalArgumentException(
                    "["
                        + SemanticTextField.INFERENCE_ID_FIELD
                        + "] on mapper ["
                        + leafName()
                        + "] of type ["
                        + CONTENT_TYPE
                        + "] must not be empty"
                );
            }
        }).alwaysSerialize();

        private final Parameter<String> searchInferenceId = Parameter.stringParam(
            SemanticTextField.SEARCH_INFERENCE_ID_FIELD,
            true,
            mapper -> ((SemanticTextFieldType) mapper.fieldType()).searchInferenceId,
            null
        ).acceptsNull().addValidator(v -> {
            if (v != null && Strings.isEmpty(v)) {
                throw new IllegalArgumentException(
                    "["
                        + SemanticTextField.SEARCH_INFERENCE_ID_FIELD
                        + "] on mapper ["
                        + leafName()
                        + "] of type ["
                        + CONTENT_TYPE
                        + "] must not be empty"
                );
            }
        });

        private final Parameter<SemanticTextField.ModelSettings> modelSettings = new Parameter<>(
            SemanticTextField.MODEL_SETTINGS_FIELD,
            true,
            () -> null,
            (n, c, o) -> SemanticTextField.parseModelSettingsFromMap(o),
            mapper -> ((SemanticTextFieldType) mapper.fieldType()).modelSettings,
            XContentBuilder::field,
            Objects::toString
        ).acceptsNull().setMergeValidator(SemanticTextFieldMapper::canMergeModelSettings);

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private Function<MapperBuilderContext, NestedObjectMapper> chunksFieldBuilder;

        public static Builder from(SemanticTextFieldMapper mapper) {
            Builder builder = new Builder(mapper.leafName(), mapper.fieldType().getChunksField().bitsetProducer(), mapper.indexSettings);
            builder.init(mapper);
            return builder;
        }

        public Builder(String name, Function<Query, BitSetProducer> bitSetProducer, IndexSettings indexSettings) {
            super(name);
            this.indexSettings = indexSettings;
            this.chunksFieldBuilder = c -> createChunksField(modelSettings.get(), bitSetProducer, indexSettings).build(c);
        }

        public Builder setInferenceId(String id) {
            this.inferenceId.setValue(id);
            return this;
        }

        public Builder setSearchInferenceId(String id) {
            this.searchInferenceId.setValue(id);
            return this;
        }

        public Builder setModelSettings(SemanticTextField.ModelSettings value) {
            this.modelSettings.setValue(value);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { inferenceId, searchInferenceId, modelSettings, meta };
        }

        @Override
        protected void merge(FieldMapper mergeWith, Conflicts conflicts, MapperMergeContext mapperMergeContext) {
            SemanticTextFieldMapper semanticMergeWith = (SemanticTextFieldMapper) mergeWith;
            semanticMergeWith = copySettings(semanticMergeWith, mapperMergeContext);

            super.merge(semanticMergeWith, conflicts, mapperMergeContext);
            conflicts.check();
            var context = mapperMergeContext.createChildContext(semanticMergeWith.leafName(), ObjectMapper.Dynamic.FALSE);
            var inferenceField = chunksFieldBuilder.apply(context.getMapperBuilderContext());
            var mergedInferenceField = inferenceField.merge(semanticMergeWith.fieldType().getChunksField(), context);
            chunksFieldBuilder = c -> (NestedObjectMapper) mergedInferenceField;
        }

        @Override
        public SemanticTextFieldMapper build(MapperBuilderContext context) {
            if (copyTo.copyToFields().isEmpty() == false) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + leafName() + "] does not support [copy_to]");
            }
            if (multiFieldsBuilder.hasMultiFields()) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + leafName() + "] does not support multi-fields");
            }
            final String fullName = context.buildFullName(leafName());

            if (context.isInNestedContext()) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + fullName + "] cannot be nested");
            }
            var childContext = context.createChildContext(leafName(), ObjectMapper.Dynamic.FALSE);
            final NestedObjectMapper chunksField = chunksFieldBuilder.apply(childContext);

            return new SemanticTextFieldMapper(
                leafName(),
                new SemanticTextFieldType(
                    fullName,
                    inferenceId.getValue(),
                    searchInferenceId.getValue(),
                    modelSettings.getValue(),
                    chunksField,
                    meta.getValue()
                ),
                builderParams(this, context),
                indexSettings
            );
        }

        /**
         * As necessary, copy settings from this builder to the passed-in mapper.
         * Used to preserve {@link SemanticTextField.ModelSettings} when updating a semantic text mapping to one where the model settings
         * are not specified.
         *
         * @param mapper The mapper
         * @return A mapper with the copied settings applied
         */
        private SemanticTextFieldMapper copySettings(SemanticTextFieldMapper mapper, MapperMergeContext mapperMergeContext) {
            SemanticTextFieldMapper returnedMapper = mapper;
            if (mapper.fieldType().getModelSettings() == null) {
                Builder builder = from(mapper);
                builder.setModelSettings(modelSettings.getValue());
                returnedMapper = builder.build(mapperMergeContext.getMapperBuilderContext());
            }

            return returnedMapper;
        }
    }

    private SemanticTextFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        BuilderParams builderParams,
        IndexSettings indexSettings
    ) {
        super(simpleName, mappedFieldType, builderParams);
        this.indexSettings = indexSettings;
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> subIterators = new ArrayList<>();
        subIterators.add(fieldType().getChunksField());
        return subIterators.iterator();
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return Builder.from(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        if (context.isWithinInferenceMetadata() == false) {
            assert indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.INFERENCE_METADATA_FIELDS);
            // ignore original text value
            return;
        }
        XContentParser parser = context.parser();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return;
        }

        XContentLocation xContentLocation = parser.getTokenLocation();
        final SemanticTextField field;
        boolean isWithinLeaf = context.path().isWithinLeafObject();
        try {
            context.path().setWithinLeafObject(true);
            field = SemanticTextField.parse(parser, new Tuple<>(fullPath(), context.parser().contentType()));
        } finally {
            context.path().setWithinLeafObject(isWithinLeaf);
        }

        final String fullFieldName = fieldType().name();
        if (field.inferenceId().equals(fieldType().getInferenceId()) == false) {
            throw new DocumentParsingException(
                xContentLocation,
                Strings.format(
                    "The configured %s [%s] for field [%s] doesn't match the %s [%s] reported in the document.",
                    SemanticTextField.INFERENCE_ID_FIELD,
                    field.inferenceId(),
                    fullFieldName,
                    SemanticTextField.INFERENCE_ID_FIELD,
                    fieldType().getInferenceId()
                )
            );
        }

        final SemanticTextFieldMapper mapper;
        if (fieldType().getModelSettings() == null) {
            context.path().remove();
            Builder builder = (Builder) new Builder(leafName(), fieldType().getChunksField().bitsetProducer(), indexSettings).init(this);
            try {
                mapper = builder.setModelSettings(field.modelSettings())
                    .setInferenceId(field.inferenceId())
                    .build(context.createDynamicMapperBuilderContext());
                context.addDynamicMapper(mapper);
            } finally {
                context.path().add(leafName());
            }
        } else {
            Conflicts conflicts = new Conflicts(fullFieldName);
            canMergeModelSettings(fieldType().getModelSettings(), field.modelSettings(), conflicts);
            try {
                conflicts.check();
            } catch (Exception exc) {
                throw new DocumentParsingException(
                    xContentLocation,
                    "Incompatible model settings for field ["
                        + fullPath()
                        + "]. Check that the "
                        + SemanticTextField.INFERENCE_ID_FIELD
                        + " is not using different model settings",
                    exc
                );
            }
            mapper = this;
        }

        var chunksField = mapper.fieldType().getChunksField();
        var embeddingsField = mapper.fieldType().getEmbeddingsField();
        var offsetsField = mapper.fieldType().getOffsetsField();
        for (var chunk : field.chunks()) {
            var nestedContext = context.createNestedContext(chunksField);
            try (
                XContentParser subParser = XContentHelper.createParserNotCompressed(
                    XContentParserConfiguration.EMPTY,
                    chunk.rawEmbeddings(),
                    context.parser().contentType()
                )
            ) {
                DocumentParserContext subContext = nestedContext.switchParser(subParser);
                subParser.nextToken();
                embeddingsField.parse(subContext);
            }

            try (
                XContentParser subParser = XContentHelper.createParserNotCompressed(
                    XContentParserConfiguration.EMPTY,
                    new BytesArray(Strings.toString(chunk.offset())),
                    context.parser().contentType()
                )
            ) {
                DocumentParserContext subContext = nestedContext.switchParser(subParser);
                subParser.nextToken();
                offsetsField.parse(subContext);
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SemanticTextFieldType fieldType() {
        return (SemanticTextFieldType) super.fieldType();
    }

    @Override
    public InferenceFieldMetadata getMetadata(Set<String> sourcePaths) {
        String[] copyFields = sourcePaths.toArray(String[]::new);
        // ensure consistent order
        Arrays.sort(copyFields);
        return new InferenceFieldMetadata(fullPath(), fieldType().getInferenceId(), fieldType().getSearchInferenceId(), copyFields);
    }

    @Override
    public Object getOriginalValue(Map<String, Object> sourceAsMap) {
        Object ret = XContentMapValues.extractValue(fullPath(), sourceAsMap);
        return SemanticTextField.nodeStringValues(fullPath(), ret);
    }

    @Override
    protected void doValidate(MappingLookup mappers) {
        int parentPathIndex = fullPath().lastIndexOf(leafName());
        if (parentPathIndex > 0) {
            // Check that the parent object field allows subobjects.
            // Subtract one from the parent path index to omit the trailing dot delimiter.
            ObjectMapper parentMapper = mappers.objectMappers().get(fullPath().substring(0, parentPathIndex - 1));
            if (parentMapper == null) {
                throw new IllegalStateException(CONTENT_TYPE + " field [" + fullPath() + "] does not have a parent object mapper");
            }

            if (parentMapper.subobjects() == ObjectMapper.Subobjects.DISABLED) {
                throw new IllegalArgumentException(
                    CONTENT_TYPE + " field [" + fullPath() + "] cannot be in an object field with subobjects disabled"
                );
            }
        }
    }

    public static class SemanticTextFieldType extends AbstractSemanticTextFieldType {
        private final String inferenceId;
        private final String searchInferenceId;
        private final SemanticTextField.ModelSettings modelSettings;
        private final NestedObjectMapper chunksField;

        public SemanticTextFieldType(
            String name,
            String inferenceId,
            String searchInferenceId,
            SemanticTextField.ModelSettings modelSettings,
            NestedObjectMapper chunksField,
            Map<String, String> meta
        ) {
            super(name, true, false, false, TextSearchInfo.NONE, meta);
            this.inferenceId = inferenceId;
            this.searchInferenceId = searchInferenceId;
            this.modelSettings = modelSettings;
            this.chunksField = chunksField;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public String getInferenceId() {
            return inferenceId;
        }

        public String getSearchInferenceId() {
            return searchInferenceId == null ? inferenceId : searchInferenceId;
        }

        public SemanticTextField.ModelSettings getModelSettings() {
            return modelSettings;
        }

        public NestedObjectMapper getChunksField() {
            return chunksField;
        }

        public FieldMapper getOffsetsField() {
            return (FieldMapper) getChunksField().getMapper(SemanticTextField.CHUNKED_OFFSET_FIELD);
        }

        public FieldMapper getEmbeddingsField() {
            return (FieldMapper) getChunksField().getMapper(SemanticTextField.CHUNKED_EMBEDDINGS_FIELD);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException(CONTENT_TYPE + " fields do not support term query");
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            if (getEmbeddingsField() == null) {
                return new MatchNoDocsQuery();
            }

            return NestedQueryBuilder.toQuery(
                (c -> getEmbeddingsField().fieldType().existsQuery(c)),
                SemanticTextField.getChunksFieldName(name()),
                ScoreMode.None,
                false,
                context
            );
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.toString(name(), context, format);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("[semantic_text] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public boolean fieldHasValue(FieldInfos fieldInfos) {
            return fieldInfos.fieldInfo(SemanticTextField.getEmbeddingsFieldName(name())) != null;
        }

        public QueryBuilder semanticQuery(InferenceResults inferenceResults, Integer requestSize, float boost, String queryName) {
            String nestedFieldPath = SemanticTextField.getChunksFieldName(name());
            String inferenceResultsFieldName = SemanticTextField.getEmbeddingsFieldName(name());
            QueryBuilder childQueryBuilder;

            if (modelSettings == null) {
                // No inference results have been indexed yet
                childQueryBuilder = new MatchNoneQueryBuilder();
            } else {
                childQueryBuilder = switch (modelSettings.taskType()) {
                    case SPARSE_EMBEDDING -> {
                        if (inferenceResults instanceof TextExpansionResults == false) {
                            throw new IllegalArgumentException(
                                generateQueryInferenceResultsTypeMismatchMessage(inferenceResults, TextExpansionResults.NAME)
                            );
                        }

                        TextExpansionResults textExpansionResults = (TextExpansionResults) inferenceResults;
                        yield new SparseVectorQueryBuilder(
                            inferenceResultsFieldName,
                            textExpansionResults.getWeightedTokens(),
                            null,
                            null,
                            null,
                            null
                        );
                    }
                    case TEXT_EMBEDDING -> {
                        if (inferenceResults instanceof MlTextEmbeddingResults == false) {
                            throw new IllegalArgumentException(
                                generateQueryInferenceResultsTypeMismatchMessage(inferenceResults, MlTextEmbeddingResults.NAME)
                            );
                        }

                        MlTextEmbeddingResults textEmbeddingResults = (MlTextEmbeddingResults) inferenceResults;
                        float[] inference = textEmbeddingResults.getInferenceAsFloat();
                        if (inference.length != modelSettings.dimensions()) {
                            throw new IllegalArgumentException(
                                generateDimensionCountMismatchMessage(inference.length, modelSettings.dimensions())
                            );
                        }

                        Integer k = requestSize;
                        if (k != null) {
                            // Ensure that k is at least the default size so that aggregations work when size is set to 0 in the request
                            k = Math.max(k, DEFAULT_SIZE);
                        }

                        yield new KnnVectorQueryBuilder(inferenceResultsFieldName, inference, k, null, null);
                    }
                    default -> throw new IllegalStateException(
                        "Field ["
                            + name()
                            + "] is configured to use an inference endpoint with an unsupported task type ["
                            + modelSettings.taskType()
                            + "]"
                    );
                };
            }

            return new NestedQueryBuilder(nestedFieldPath, childQueryBuilder, ScoreMode.Max).boost(boost).queryName(queryName);
        }

        private String generateQueryInferenceResultsTypeMismatchMessage(InferenceResults inferenceResults, String expectedResultsType) {
            StringBuilder sb = new StringBuilder(
                "Field ["
                    + name()
                    + "] expected query inference results to be of type ["
                    + expectedResultsType
                    + "],"
                    + " got ["
                    + inferenceResults.getWriteableName()
                    + "]."
            );

            return generateInvalidQueryInferenceResultsMessage(sb);
        }

        private String generateDimensionCountMismatchMessage(int inferenceDimCount, int expectedDimCount) {
            StringBuilder sb = new StringBuilder(
                "Field ["
                    + name()
                    + "] expected query inference results with "
                    + expectedDimCount
                    + " dimensions, got "
                    + inferenceDimCount
                    + " dimensions."
            );

            return generateInvalidQueryInferenceResultsMessage(sb);
        }

        private String generateInvalidQueryInferenceResultsMessage(StringBuilder baseMessageBuilder) {
            if (searchInferenceId != null && searchInferenceId.equals(inferenceId) == false) {
                baseMessageBuilder.append(
                    " Is the search inference endpoint ["
                        + searchInferenceId
                        + "] compatible with the inference endpoint ["
                        + inferenceId
                        + "]?"
                );
            } else {
                baseMessageBuilder.append(" Has the configuration for inference endpoint [" + inferenceId + "] changed?");
            }

            return baseMessageBuilder.toString();
        }

        @Override
        public BlockLoader blockLoader(MappedFieldType.BlockLoaderContext blContext) {
            SourceValueFetcher fetcher = SourceValueFetcher.toString(blContext.sourcePaths(name().concat(".text")));
            return new BlockSourceReader.BytesRefsBlockLoader(fetcher, BlockSourceReader.lookupMatchingAll());
        }
    }

    private static NestedObjectMapper.Builder createChunksField(
        @Nullable SemanticTextField.ModelSettings modelSettings,
        Function<Query, BitSetProducer> bitSetProducer,
        IndexSettings indexSettings
    ) {
        NestedObjectMapper.Builder chunksField = new NestedObjectMapper.Builder(
            SemanticTextField.CHUNKS_FIELD,
            indexSettings.getIndexVersionCreated(),
            bitSetProducer,
            indexSettings
        );
        chunksField.dynamic(ObjectMapper.Dynamic.FALSE);

        if (modelSettings != null) {
            chunksField.add(createEmbeddingsField(indexSettings.getIndexVersionCreated(), modelSettings));
        }
        chunksField.add(new OffsetSourceFieldMapper.Builder(SemanticTextField.CHUNKED_OFFSET_FIELD));
        return chunksField;
    }

    private static Mapper.Builder createEmbeddingsField(IndexVersion indexVersionCreated, SemanticTextField.ModelSettings modelSettings) {
        return switch (modelSettings.taskType()) {
            case SPARSE_EMBEDDING -> new SparseVectorFieldMapper.Builder(SemanticTextField.CHUNKED_EMBEDDINGS_FIELD);
            case TEXT_EMBEDDING -> {
                DenseVectorFieldMapper.Builder denseVectorMapperBuilder = new DenseVectorFieldMapper.Builder(
                    SemanticTextField.CHUNKED_EMBEDDINGS_FIELD,
                    indexVersionCreated
                );

                SimilarityMeasure similarity = modelSettings.similarity();
                if (similarity != null) {
                    switch (similarity) {
                        case COSINE -> denseVectorMapperBuilder.similarity(DenseVectorFieldMapper.VectorSimilarity.COSINE);
                        case DOT_PRODUCT -> denseVectorMapperBuilder.similarity(DenseVectorFieldMapper.VectorSimilarity.DOT_PRODUCT);
                        case L2_NORM -> denseVectorMapperBuilder.similarity(DenseVectorFieldMapper.VectorSimilarity.L2_NORM);
                        default -> throw new IllegalArgumentException(
                            "Unknown similarity measure in model_settings [" + similarity.name() + "]"
                        );
                    }
                }
                denseVectorMapperBuilder.dimensions(modelSettings.dimensions());
                denseVectorMapperBuilder.elementType(modelSettings.elementType());

                yield denseVectorMapperBuilder;
            }
            default -> throw new IllegalArgumentException("Invalid task_type in model_settings [" + modelSettings.taskType().name() + "]");
        };
    }

    private static boolean canMergeModelSettings(
        SemanticTextField.ModelSettings previous,
        SemanticTextField.ModelSettings current,
        Conflicts conflicts
    ) {
        if (Objects.equals(previous, current)) {
            return true;
        }
        if (previous == null) {
            return true;
        }
        conflicts.addConflict("model_settings", "");
        return false;
    }
}
