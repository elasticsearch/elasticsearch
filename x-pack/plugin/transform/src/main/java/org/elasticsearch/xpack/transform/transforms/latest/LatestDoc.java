/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.latest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestDocConfig;
import org.elasticsearch.xpack.transform.transforms.Function;
import org.elasticsearch.xpack.transform.transforms.IDGenerator;
import org.elasticsearch.xpack.transform.transforms.pivot.SchemaUtil;
import org.elasticsearch.xpack.transform.transforms.common.DocumentConversionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class LatestDoc implements Function {

    public static final int DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE = 5000;
    public static final int TEST_QUERY_PAGE_SIZE = 50;

    private static final String COMPOSITE_AGGREGATION_NAME = "_transform";
    private static final String TOP_HITS_AGGREGATION_NAME = "_top_hits";
    private static final Logger logger = LogManager.getLogger(LatestDoc.class);

    private final LatestDocConfig config;

    // objects for re-using
    private final CompositeAggregationBuilder cachedCompositeAggregation;

    public LatestDoc(LatestDocConfig config) {
        this.config = config;
        this.cachedCompositeAggregation = createCompositeAggregation(config);
    }

    private static CompositeAggregationBuilder createCompositeAggregation(LatestDocConfig config) {
        assert config.getSort().size() == 1 && config.getSort().get(0) instanceof FieldSortBuilder;

        CompositeAggregationBuilder compositeAggregation;
        try (XContentBuilder builder = jsonBuilder()) {
            config.toCompositeAggXContent(builder);
            XContentParser parser = builder.generator()
                .contentType()
                .xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
            compositeAggregation = CompositeAggregationBuilder.PARSER.parse(parser, COMPOSITE_AGGREGATION_NAME);
        } catch (IOException e) {
            throw new RuntimeException(
                TransformMessages.getMessage(TransformMessages.TRANSFORM_FAILED_TO_CREATE_COMPOSITE_AGGREGATION, "latest_doc"), e);
        }
        TopHitsAggregationBuilder topHitsAgg =
            AggregationBuilders.topHits(TOP_HITS_AGGREGATION_NAME)
                .size(1)  // we are only interested in the top-1
                .sorts(config.getSort());  // we copy the sort config directly from the function config
        compositeAggregation.subAggregation(topHitsAgg);
        return compositeAggregation;
    }

    @Override
    public int getInitialPageSize() {
        return DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE;
    }

    private SearchRequest buildSearchRequest(SourceConfig sourceConfig, Map<String, Object> position, int pageSize) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        buildSearchQuery(sourceBuilder, null, pageSize);
        sourceBuilder.query(sourceConfig.getQueryConfig().getQuery());
        SearchRequest searchRequest =
            new SearchRequest(sourceConfig.getIndex())
                .source(sourceBuilder)
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        logger.trace("Search request: {}", searchRequest);
        return searchRequest;
    }

    @Override
    public SearchSourceBuilder buildSearchQuery(SearchSourceBuilder builder, Map<String, Object> position, int pageSize) {
        cachedCompositeAggregation.aggregateAfter(position);
        cachedCompositeAggregation.size(pageSize);
        return builder.size(0).aggregation(cachedCompositeAggregation);
    }

    @Override
    public ChangeCollector buildChangeCollector(String synchronizationField) {
        return new LatestDocChangeCollector(synchronizationField);
    }

    private Stream<Map<String, Object>> extractResults(CompositeAggregation compositeAgg, TransformIndexerStats transformIndexerStats) {
        return compositeAgg.getBuckets().stream()
            .map(bucket -> {
                transformIndexerStats.incrementNumDocuments(bucket.getDocCount());
                TopHits topHits = bucket.getAggregations().get(TOP_HITS_AGGREGATION_NAME);
                assert topHits.getHits().getHits().length == 1;
                Map<String, Object> document = topHits.getHits().getHits()[0].getSourceAsMap();

                // generator to create unique but deterministic document ids, so we
                // - do not create duplicates if we re-run after failure
                // - update documents
                IDGenerator idGen = new IDGenerator();
                config.getUniqueKey().forEach(field -> idGen.add(field, bucket.getKey().get(field)));

                document.put(TransformField.DOCUMENT_ID_FIELD, idGen.getID());
                return document;
            });
    }

    @Override
    public Tuple<Stream<IndexRequest>, Map<String, Object>> processSearchResponse(
        SearchResponse searchResponse,
        String destinationIndex,
        String destinationPipeline,
        Map<String, String> fieldTypeMap,
        TransformIndexerStats stats
    ) {
        Aggregations aggregations = searchResponse.getAggregations();

        // Treat this as a "we reached the end".
        // This should only happen when all underlying indices have gone away. Consequently, there is no more data to read.
        if (aggregations == null) {
            return null;
        }

        CompositeAggregation compositeAgg = aggregations.get(COMPOSITE_AGGREGATION_NAME);
        if (compositeAgg == null || compositeAgg.getBuckets().isEmpty()) {
            return null;
        }

        Stream<IndexRequest> indexRequestStream =
            extractResults(compositeAgg, stats)
                .map(document -> DocumentConversionUtils.convertDocumentToIndexRequest(document, destinationIndex, destinationPipeline));
        return Tuple.tuple(indexRequestStream, compositeAgg.afterKey());
    }

    @Override
    public void validateQuery(Client client, SourceConfig sourceConfig, ActionListener<Boolean> listener) {
        SearchRequest searchRequest = buildSearchRequest(sourceConfig, null, TEST_QUERY_PAGE_SIZE);

        client.execute(SearchAction.INSTANCE, searchRequest, ActionListener.wrap(response -> {
            if (response == null) {
                listener.onFailure(
                    new ElasticsearchStatusException("Unexpected null response from test query", RestStatus.SERVICE_UNAVAILABLE)
                );
                return;
            }
            if (response.status() != RestStatus.OK) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Unexpected status from response of test query: {}", response.status(), response.status())
                );
                return;
            }
            listener.onResponse(true);
        }, e -> {
            Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
            RestStatus status = unwrapped instanceof ElasticsearchException
                ? ((ElasticsearchException) unwrapped).status()
                : RestStatus.SERVICE_UNAVAILABLE;
            listener.onFailure(new ElasticsearchStatusException("Failed to test query", status, unwrapped));
        }));
    }

    @Override
    public void validateConfig(ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    @Override
    public void deduceMappings(Client client, SourceConfig sourceConfig, ActionListener<Map<String, String>> listener) {
        FieldCapabilitiesRequest fieldCapabilitiesRequest =
            new FieldCapabilitiesRequest().indices(sourceConfig.getIndex())
                .fields("*")
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        client.execute(
            FieldCapabilitiesAction.INSTANCE,
            fieldCapabilitiesRequest,
            ActionListener.wrap(
                response -> listener.onResponse(
                    SchemaUtil.extractFieldMappings(response).entrySet().stream()
                        .filter(not(e -> e.getKey().startsWith("_")))
                        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue))
                ),
                listener::onFailure)
        );
    }

    @Override
    public void preview(
        Client client,
        Map<String, String> headers,
        SourceConfig sourceConfig,
        Map<String, String> mappings,
        int numberOfBuckets,
        ActionListener<List<Map<String, Object>>> listener
    ) {
        ClientHelper.executeWithHeadersAsync(
            headers,
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            SearchAction.INSTANCE,
            buildSearchRequest(sourceConfig, null, numberOfBuckets),
            ActionListener.wrap(r -> {
                Aggregations aggregations = r.getAggregations();
                if (aggregations == null) {
                    listener.onFailure(
                        new ElasticsearchStatusException("Source indices have been deleted or closed.", RestStatus.BAD_REQUEST));
                    return;
                }
                CompositeAggregation compositeAgg = aggregations.get(COMPOSITE_AGGREGATION_NAME);
                TransformIndexerStats stats = new TransformIndexerStats();

                List<Map<String, Object>> docs =
                    extractResults(compositeAgg, stats)
                        // remove all internal fields
                        .peek(doc -> doc.keySet().removeIf(k -> k.startsWith("_")))
                        .collect(Collectors.toList());

                listener.onResponse(docs);
            }, listener::onFailure)
        );
    }

    @Override
    public SearchSourceBuilder buildSearchQueryForInitialProgress(SearchSourceBuilder searchSourceBuilder) {
        BoolQueryBuilder existsClauses = QueryBuilders.boolQuery();
        config.getUniqueKey().forEach(field -> existsClauses.must(QueryBuilders.existsQuery(field)));
        return searchSourceBuilder.query(existsClauses).size(0).trackTotalHits(true);
    }

    @Override
    public void getInitialProgressFromResponse(SearchResponse response, ActionListener<TransformProgress> progressListener) {
        progressListener.onResponse(new TransformProgress(response.getHits().getTotalHits().value, 0L, 0L));
    }
}
