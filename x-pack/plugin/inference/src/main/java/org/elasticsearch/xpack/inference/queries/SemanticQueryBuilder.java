/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.TransportVersions.SEMANTIC_QUERY_MULTIPLE_INFERENCE_IDS;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class SemanticQueryBuilder extends AbstractQueryBuilder<SemanticQueryBuilder> {
    public static final String NAME = "semantic";

    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField LENIENT_FIELD = new ParseField("lenient");

    private static final ConstructingObjectParser<SemanticQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> new SemanticQueryBuilder((String) args[0], (String) args[1], (Boolean) args[2])
    );

    private static class LegacyInferenceResultsMap extends AbstractMap<String, InferenceResults> {
        private static final String PLACEHOLDER_INFERENCE_ID = ".placeholder";

        private final Map<String, InferenceResults> wrappedMap;

        private LegacyInferenceResultsMap(InferenceResults inferenceResults) {
            this.wrappedMap = Map.of(PLACEHOLDER_INFERENCE_ID, inferenceResults);
        }

        private InferenceResults getInferenceResults() {
            return wrappedMap.get(PLACEHOLDER_INFERENCE_ID);
        }

        @Override
        public Set<Entry<String, InferenceResults>> entrySet() {
            return wrappedMap.entrySet();
        }

        @Override
        public InferenceResults get(Object key) {
            return wrappedMap.get(key);
        }

        @Override
        public boolean containsKey(Object key) {
            return wrappedMap.containsKey(key);
        }
    }

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareString(constructorArg(), QUERY_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), LENIENT_FIELD);
        declareStandardFields(PARSER);
    }

    private final String fieldName;
    private final String query;
    private final Map<String, InferenceResults> inferenceResultsMap;
    private final boolean noInferenceResults;
    private final Boolean lenient;

    public SemanticQueryBuilder(String fieldName, String query) {
        this(fieldName, query, null);
    }

    public SemanticQueryBuilder(String fieldName, String query, Boolean lenient) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + FIELD_FIELD.getPreferredName() + " value");
        }
        if (query == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + QUERY_FIELD.getPreferredName() + " value");
        }
        this.fieldName = fieldName;
        this.query = query;
        this.inferenceResultsMap = null;
        this.noInferenceResults = false;
        this.lenient = lenient;
    }

    public SemanticQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.query = in.readString();
        if (in.getTransportVersion().onOrAfter(SEMANTIC_QUERY_MULTIPLE_INFERENCE_IDS)) {
            if (in.readBoolean()) {
                this.inferenceResultsMap = in.readMap((input) -> input.readNamedWriteable(InferenceResults.class));
            } else {
                this.inferenceResultsMap = null;
            }
        } else {
            InferenceResults inferenceResults = in.readOptionalNamedWriteable(InferenceResults.class);
            if (inferenceResults != null) {
                this.inferenceResultsMap = new LegacyInferenceResultsMap(inferenceResults);
            } else {
                this.inferenceResultsMap = null;
            }
        }
        this.noInferenceResults = in.readBoolean();
        if (in.getTransportVersion().onOrAfter(TransportVersions.SEMANTIC_QUERY_LENIENT)) {
            this.lenient = in.readOptionalBoolean();
        } else {
            this.lenient = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeString(query);
        if (out.getTransportVersion().onOrAfter(SEMANTIC_QUERY_MULTIPLE_INFERENCE_IDS)) {
            if (inferenceResultsMap != null) {
                out.writeBoolean(true);
                out.writeMap(inferenceResultsMap, StreamOutput::writeNamedWriteable);
            } else {
                out.writeBoolean(false);
            }
        } else {
            InferenceResults inferenceResults = null;
            int inferenceResultsMapSize = inferenceResultsMap.size();
            if (inferenceResultsMapSize > 1) {
                throw new IllegalArgumentException("Cannot query multiple inference IDs in a mixed-version cluster");
            } else if (inferenceResultsMapSize == 1) {
                inferenceResults = inferenceResultsMap.values().iterator().next();
            }

            out.writeOptionalNamedWriteable(inferenceResults);
        }
        out.writeBoolean(noInferenceResults);
        if (out.getTransportVersion().onOrAfter(TransportVersions.SEMANTIC_QUERY_LENIENT)) {
            out.writeOptionalBoolean(lenient);
        }
    }

    private SemanticQueryBuilder(
        SemanticQueryBuilder other,
        Map<String, InferenceResults> inferenceResultsMap,
        boolean noInferenceResults
    ) {
        this.fieldName = other.fieldName;
        this.query = other.query;
        this.boost = other.boost;
        this.queryName = other.queryName;
        this.inferenceResultsMap = inferenceResultsMap;
        this.noInferenceResults = noInferenceResults;
        this.lenient = other.lenient;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getQuery() {
        return query;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    public static SemanticQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), fieldName);
        builder.field(QUERY_FIELD.getPreferredName(), query);
        if (lenient != null) {
            builder.field(LENIENT_FIELD.getPreferredName(), lenient);
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        SearchExecutionContext searchExecutionContext = queryRewriteContext.convertToSearchExecutionContext();
        if (searchExecutionContext != null) {
            return doRewriteBuildSemanticQuery(searchExecutionContext);
        }

        return doRewriteGetInferenceResults(queryRewriteContext);
    }

    private QueryBuilder doRewriteBuildSemanticQuery(SearchExecutionContext searchExecutionContext) {
        MappedFieldType fieldType = searchExecutionContext.getFieldType(fieldName);
        if (fieldType == null) {
            return new MatchNoneQueryBuilder();
        } else if (fieldType instanceof SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType) {
            if (inferenceResultsMap == null) {
                // This should never happen, but throw on it in case it ever does
                throw new IllegalStateException(
                    "No inference results set for [" + semanticTextFieldType.typeName() + "] field [" + fieldName + "]"
                );
            }

            final InferenceResults inferenceResults;
            String inferenceId = semanticTextFieldType.getSearchInferenceId();
            if (inferenceResultsMap instanceof LegacyInferenceResultsMap legacyInferenceResultsMap) {
                // We are reading inference results from a coordinator node using an old transport version
                inferenceResults = legacyInferenceResultsMap.getInferenceResults();
            } else {
                inferenceResults = inferenceResultsMap.get(inferenceId);
            }

            if (inferenceResults == null) {
                throw new IllegalStateException(
                    "No inference results set for ["
                        + semanticTextFieldType.typeName()
                        + "] field ["
                        + fieldName
                        + "] with inference ID ["
                        + inferenceId
                        + "]"
                );
            } else if (inferenceResults instanceof ErrorInferenceResults errorInferenceResults) {
                throw new IllegalStateException(
                    "Field ["
                        + fieldName
                        + "] with inference ID ["
                        + inferenceId
                        + "] query inference error: "
                        + errorInferenceResults.getException().getMessage(),
                    errorInferenceResults.getException()
                );
            } else if (inferenceResults instanceof WarningInferenceResults warningInferenceResults) {
                throw new IllegalStateException(
                    "Field ["
                        + fieldName
                        + "] with inference ID ["
                        + inferenceId
                        + "] query inference warning: "
                        + warningInferenceResults.getWarning()
                );
            }

            return semanticTextFieldType.semanticQuery(inferenceResults, searchExecutionContext.requestSize(), boost(), queryName());
        } else if (lenient != null && lenient) {
            return new MatchNoneQueryBuilder();
        } else {
            throw new IllegalArgumentException(
                "Field [" + fieldName + "] of type [" + fieldType.typeName() + "] does not support " + NAME + " queries"
            );
        }
    }

    private SemanticQueryBuilder doRewriteGetInferenceResults(QueryRewriteContext queryRewriteContext) {
        if (inferenceResultsMap != null || noInferenceResults) {
            return this;
        }

        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        if (resolvedIndices == null) {
            throw new IllegalStateException(
                "Rewriting on the coordinator node requires a query rewrite context with non-null resolved indices"
            );
        } else if (resolvedIndices.getRemoteClusterIndices().isEmpty() == false) {
            throw new IllegalArgumentException(NAME + " query does not support cross-cluster search");
        }

        Set<String> inferenceIds = getInferenceIdsForForField(resolvedIndices.getConcreteLocalIndicesMetadata().values(), fieldName);
        Map<String, InferenceResults> inferenceResultsMap = new ConcurrentHashMap<>(inferenceIds.size());

        // The inference ID set can be empty if either the field name or index name(s) are invalid (or both).
        // If this happens, we set the "no inference results" flag to true so the rewrite process can continue.
        // Invalid index names will be handled in the transport layer, when the query is sent to the shard.
        // Invalid field names will be handled when the query is re-written on the shard, where we have access to the index mappings.
        boolean noInferenceResults = inferenceIds.isEmpty();

        for (String inferenceId : inferenceIds) {
            InferenceAction.Request inferenceRequest = new InferenceAction.Request(
                TaskType.ANY,
                inferenceId,
                null,
                List.of(query),
                Map.of(),
                InputType.SEARCH,
                InferModelAction.Request.DEFAULT_TIMEOUT_FOR_API,
                false
            );

            queryRewriteContext.registerAsyncAction(
                (client, listener) -> executeAsyncWithOrigin(
                    client,
                    ML_ORIGIN,
                    InferenceAction.INSTANCE,
                    inferenceRequest,
                    listener.delegateFailureAndWrap((l, inferenceResponse) -> {
                        inferenceResultsMap.put(
                            inferenceId,
                            validateAndConvertInferenceResults(inferenceResponse.getResults(), fieldName, inferenceId)
                        );
                        l.onResponse(null);
                    })
                )
            );
        }

        return new SemanticQueryBuilder(this, noInferenceResults ? null : inferenceResultsMap, noInferenceResults);
    }

    private static InferenceResults validateAndConvertInferenceResults(
        InferenceServiceResults inferenceServiceResults,
        String fieldName,
        String inferenceId
    ) {
        List<? extends InferenceResults> inferenceResultsList = inferenceServiceResults.transformToCoordinationFormat();
        if (inferenceResultsList.isEmpty()) {
            return new ErrorInferenceResults(
                new IllegalArgumentException(
                    "No inference results retrieved for field [" + fieldName + "] with inference ID [" + inferenceId + "]"
                )
            );
        } else if (inferenceResultsList.size() > 1) {
            // The inference call should truncate if the query is too large.
            // Thus, if we receive more than one inference result, it is a server-side error.
            return new ErrorInferenceResults(
                new IllegalStateException(
                    inferenceResultsList.size()
                        + " inference results retrieved for field ["
                        + fieldName
                        + "] with inference ID ["
                        + inferenceId
                        + "]"
                )
            );
        }

        InferenceResults inferenceResults = inferenceResultsList.get(0);
        if (inferenceResults instanceof TextExpansionResults == false
            && inferenceResults instanceof MlTextEmbeddingResults == false
            && inferenceResults instanceof ErrorInferenceResults == false
            && inferenceResults instanceof WarningInferenceResults == false) {
            return new ErrorInferenceResults(
                new IllegalArgumentException(
                    "Field ["
                        + fieldName
                        + "] with inference ID ["
                        + inferenceId
                        + "] expected query inference results to be of type ["
                        + TextExpansionResults.NAME
                        + "] or ["
                        + MlTextEmbeddingResults.NAME
                        + "], got ["
                        + inferenceResults.getWriteableName()
                        + "]. Has the inference endpoint configuration changed?"
                )
            );
        }

        return inferenceResults;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        throw new IllegalStateException(NAME + " should have been rewritten to another query type");
    }

    private static Set<String> getInferenceIdsForForField(Collection<IndexMetadata> indexMetadataCollection, String fieldName) {
        Set<String> inferenceIds = new HashSet<>();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            InferenceFieldMetadata inferenceFieldMetadata = indexMetadata.getInferenceFields().get(fieldName);
            String indexInferenceId = inferenceFieldMetadata != null ? inferenceFieldMetadata.getSearchInferenceId() : null;
            if (indexInferenceId != null) {
                inferenceIds.add(indexInferenceId);
            }
        }

        return inferenceIds;
    }

    @Override
    protected boolean doEquals(SemanticQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(query, other.query)
            && Objects.equals(inferenceResultsMap, other.inferenceResultsMap)
            && Objects.equals(noInferenceResults, other.noInferenceResults)
            && Objects.equals(lenient, other.lenient);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, query, inferenceResultsMap, noInferenceResults, lenient);
    }
}
