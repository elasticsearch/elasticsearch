/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.enrich.EnrichProcessorFactory.EnrichSpecification;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ExactMatchProcessorTests extends ESTestCase {

    public void testBasics() throws Exception {
        MockSearchFunction mockSearch = mockedSearchFunction(Map.of("elastic.co", Map.of("globalRank", 451, "tldRank",23, "tld", "co")));
        ExactMatchProcessor processor = new ExactMatchProcessor("_tag", mockSearch, "_name", "domain", false,
            List.of(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));
        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
            Map.of("domain", "elastic.co"));
        // Run
        IngestDocument[] holder = new IngestDocument[1];
        processor.execute(ingestDocument, (result, e) -> holder[0] = result);
        assertThat(holder[0], notNullValue());
        // Check request
        SearchRequest request = mockSearch.getCapturedRequest();
        assertThat(request.indices().length, equalTo(1));
        assertThat(request.indices()[0], equalTo(".enrich-_name"));
        assertThat(request.preference(), equalTo(Preference.LOCAL.type()));
        assertThat(request.source().size(), equalTo(1));
        assertThat(request.source().trackScores(), equalTo(false));
        assertThat(request.source().fetchSource().fetchSource(), equalTo(true));
        assertThat(request.source().fetchSource().includes(), arrayContainingInAnyOrder("tldRank", "tld"));
        assertThat(request.source().query(), instanceOf(ConstantScoreQueryBuilder.class));
        assertThat(((ConstantScoreQueryBuilder) request.source().query()).innerQuery(), instanceOf(TermQueryBuilder.class));
        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) ((ConstantScoreQueryBuilder) request.source().query()).innerQuery();
        assertThat(termQueryBuilder.fieldName(), equalTo("domain"));
        assertThat(termQueryBuilder.value(), equalTo("elastic.co"));
        // Check result
        assertThat(ingestDocument.getFieldValue("tld_rank", Integer.class), equalTo(23));
        assertThat(ingestDocument.getFieldValue("tld", String.class), equalTo("co"));
    }

    public void testNoMatch() throws Exception {
        MockSearchFunction mockSearch = mockedSearchFunction();
        ExactMatchProcessor processor = new ExactMatchProcessor("_tag", mockSearch, "_name", "domain", false,
            List.of(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));
        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
            Map.of("domain", "elastic.com"));
        int numProperties = ingestDocument.getSourceAndMetadata().size();
        // Run
        IngestDocument[] holder = new IngestDocument[1];
        processor.execute(ingestDocument, (result, e) -> holder[0] = result);
        assertThat(holder[0], notNullValue());
        // Check request
        SearchRequest request = mockSearch.getCapturedRequest();
        assertThat(request.indices().length, equalTo(1));
        assertThat(request.indices()[0], equalTo(".enrich-_name"));
        assertThat(request.preference(), equalTo(Preference.LOCAL.type()));
        assertThat(request.source().size(), equalTo(1));
        assertThat(request.source().trackScores(), equalTo(false));
        assertThat(request.source().fetchSource().fetchSource(), equalTo(true));
        assertThat(request.source().fetchSource().includes(), arrayContainingInAnyOrder("tldRank", "tld"));
        assertThat(request.source().query(), instanceOf(ConstantScoreQueryBuilder.class));
        assertThat(((ConstantScoreQueryBuilder) request.source().query()).innerQuery(), instanceOf(TermQueryBuilder.class));
        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) ((ConstantScoreQueryBuilder) request.source().query()).innerQuery();
        assertThat(termQueryBuilder.fieldName(), equalTo("domain"));
        assertThat(termQueryBuilder.value(), equalTo("elastic.com"));
        // Check result
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(numProperties));
    }

    public void testSearchFailure() throws Exception {
        String indexName = ".enrich-_name";
        MockSearchFunction mockSearch = mockedSearchFunction(new IndexNotFoundException(indexName));
        ExactMatchProcessor processor = new ExactMatchProcessor("_tag", mockSearch, "_name", "domain", false,
            List.of(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));
        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
            Map.of("domain", "elastic.com"));
        // Run
        IngestDocument[] resultHolder = new IngestDocument[1];
        Exception[] exceptionHolder = new Exception[1];
        processor.execute(ingestDocument, (result, e) -> {
            resultHolder[0] = result;
            exceptionHolder[0] = e;
        });
        assertThat(resultHolder[0], nullValue());
        assertThat(exceptionHolder[0], notNullValue());
        assertThat(exceptionHolder[0], instanceOf(IndexNotFoundException.class));
        // Check request
        SearchRequest request = mockSearch.getCapturedRequest();
        assertThat(request.indices().length, equalTo(1));
        assertThat(request.indices()[0], equalTo(".enrich-_name"));
        assertThat(request.preference(), equalTo(Preference.LOCAL.type()));
        assertThat(request.source().size(), equalTo(1));
        assertThat(request.source().trackScores(), equalTo(false));
        assertThat(request.source().fetchSource().fetchSource(), equalTo(true));
        assertThat(request.source().fetchSource().includes(), arrayContainingInAnyOrder("tldRank", "tld"));
        assertThat(request.source().query(), instanceOf(ConstantScoreQueryBuilder.class));
        assertThat(((ConstantScoreQueryBuilder) request.source().query()).innerQuery(), instanceOf(TermQueryBuilder.class));
        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) ((ConstantScoreQueryBuilder) request.source().query()).innerQuery();
        assertThat(termQueryBuilder.fieldName(), equalTo("domain"));
        assertThat(termQueryBuilder.value(), equalTo("elastic.com"));
        // Check result
        assertThat(exceptionHolder[0].getMessage(), equalTo("no such index [" + indexName + "]"));
    }

    public void testIgnoreKeyMissing() throws Exception {
        {
            ExactMatchProcessor processor = new ExactMatchProcessor("_tag", mockedSearchFunction(), "_name", "domain",
                true, List.of(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));
            IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL, Map.of());

            assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(6));
            IngestDocument[] holder = new IngestDocument[1];
            processor.execute(ingestDocument, (result, e) -> holder[0] = result);
            assertThat(holder[0], notNullValue());
            assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(6));
        }
        {
            ExactMatchProcessor processor = new ExactMatchProcessor("_tag", mockedSearchFunction(), "_name", "domain",
                false, List.of(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));
            IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL, Map.of());
            IngestDocument[] resultHolder = new IngestDocument[1];
            Exception[] exceptionHolder = new Exception[1];
            processor.execute(ingestDocument, (result, e) -> {
                resultHolder[0] = result;
                exceptionHolder[0] = e;
            });
            assertThat(resultHolder[0], nullValue());
            assertThat(exceptionHolder[0], notNullValue());
            assertThat(exceptionHolder[0], instanceOf(IllegalArgumentException.class));
        }
    }

    private static final class MockSearchFunction implements BiConsumer<SearchRequest, BiConsumer<SearchResponse, Exception>>  {
        private final SearchResponse mockResponse;
        private final SetOnce<SearchRequest> capturedRequest;
        private final Exception exception;

        MockSearchFunction(SearchResponse mockResponse) {
            this.mockResponse = mockResponse;
            this.exception = null;
            this.capturedRequest = new SetOnce<>();
        }

        MockSearchFunction(Exception exception) {
            this.mockResponse = null;
            this.exception = exception;
            this.capturedRequest = new SetOnce<>();
        }

        @Override
        public void accept(SearchRequest request, BiConsumer<SearchResponse, Exception> handler) {
            capturedRequest.set(request);
            if (exception != null) {
                handler.accept(null, exception);
            } else {
                handler.accept(mockResponse, null);
            }
        }

        SearchRequest getCapturedRequest() {
            return capturedRequest.get();
        }
    }

    public MockSearchFunction mockedSearchFunction() {
        return new MockSearchFunction(mockResponse(Collections.emptyMap()));
    }

    public MockSearchFunction mockedSearchFunction(Exception exception) {
        return new MockSearchFunction(exception);
    }

    public MockSearchFunction mockedSearchFunction(Map<String, Map<String, ?>> documents) {
        return new MockSearchFunction(mockResponse(documents));
    }

    public SearchResponse mockResponse(Map<String, Map<String, ?>> documents) {
        SearchHit[] searchHits = documents.entrySet().stream().map(e -> {
            SearchHit searchHit = new SearchHit(randomInt(100), e.getKey(), new Text(MapperService.SINGLE_MAPPING_NAME),
                Collections.emptyMap());
            try (XContentBuilder builder = XContentBuilder.builder(XContentType.SMILE.xContent())) {
                builder.map(e.getValue());
                builder.flush();
                ByteArrayOutputStream outputStream = (ByteArrayOutputStream) builder.getOutputStream();
                searchHit.sourceRef(new BytesArray(outputStream.toByteArray()));
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
            return searchHit;
        }).toArray(SearchHit[]::new);
        return new SearchResponse(new SearchResponseSections(
            new SearchHits(searchHits, new TotalHits(documents.size(), TotalHits.Relation.EQUAL_TO), 1.0f),
            new Aggregations(Collections.emptyList()), new Suggest(Collections.emptyList()),
            false, false, null, 1), null, 1, 1, 0, 1, ShardSearchFailure.EMPTY_ARRAY, new SearchResponse.Clusters(1, 1, 0));
    }
}
