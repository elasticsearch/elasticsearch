/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.action.admin.cluster.stats.SearchUsageStats;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.KnnRetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TextSimilarityRankRetrieverTelemetryIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "test_index";

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InferencePlugin.class, XPackPlugin.class, TextSimilarityTestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("xpack.license.self_generated.type", "trial")
            .build();
    }

    @Before
    public void setup() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", 1)
            .field("index", true)
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", "hnsw")
            .endObject()
            .endObject()
            .startObject("text")
            .field("type", "text")
            .endObject()
            .startObject("integer")
            .field("type", "integer")
            .endObject()
            .startObject("topic")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();

        assertAcked(prepareCreate(INDEX_NAME).setMapping(builder));
        ensureGreen(INDEX_NAME);
    }

    public void testTelemetryForRRFRetriever() throws IOException {

        // search#1 - this will record 1 entry for "retriever" in `sections`, and 1 for "knn" under `retrievers`
        {
            Request request = new Request("GET", INDEX_NAME + "/_search");
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.retriever(new KnnRetrieverBuilder("vector", new float[] { 1.0f }, null, 10, 15, null));
            request.setJsonEntity(Strings.toString(source));
            getRestClient().performRequest(request);
        }

        // search#2 - this will record 1 entry for "retriever" in `sections`, 1 for "standard" under `retrievers`, and 1 for "range" under
        // `queries`
        {
            Request request = new Request("GET", INDEX_NAME + "/_search");
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.retriever(new StandardRetrieverBuilder(QueryBuilders.rangeQuery("integer").gte(2)));
            request.setJsonEntity(Strings.toString(source));
            getRestClient().performRequest(request);
        }

        // search#3 - this will record 1 entry for "retriever" in `sections`, and 1 for "standard" under `retrievers`, and 1 for "knn" under
        // `queries`
        {
            Request request = new Request("GET", INDEX_NAME + "/_search");
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.retriever(new StandardRetrieverBuilder(new KnnVectorQueryBuilder("vector", new float[] { 1.0f }, 10, 15, null)));
            request.setJsonEntity(Strings.toString(source));
            getRestClient().performRequest(request);
        }

        // search#4 - this will record 1 entry for "retriever" in `sections`, and 1 for "standard" under `retrievers`, and 1 for "term"
        // under `queries`
        {
            Request request = new Request("GET", INDEX_NAME + "/_search");
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.retriever(new StandardRetrieverBuilder(QueryBuilders.termQuery("topic", "foo")));
            request.setJsonEntity(Strings.toString(source));
            getRestClient().performRequest(request);
        }

        // search#5 - this will record 1 entry for "retriever" in `sections`, and 1 for "text_similarity_reranker" under `retrievers`, as well as
        // 1 "standard" under `retrievers`, and eventually 1 for "match" under `queries`
        {
            Request request = new Request("GET", INDEX_NAME + "/_search");
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.retriever(
                new TextSimilarityRankRetrieverBuilder(
                    List.of(
                        new CompoundRetrieverBuilder.RetrieverSource(
                            new StandardRetrieverBuilder(QueryBuilders.matchQuery("text", "foo")),
                            null
                        )
                    )
                ),
                10,
                10
            );
            request.setJsonEntity(Strings.toString(source));
            getRestClient().performRequest(request);
        }

        // search#6 - this will record 1 entry for "knn" in `sections`
        {
            Request request = new Request("GET", INDEX_NAME + "/_search");
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.knnSearch(List.of(new KnnSearchBuilder("vector", new float[] { 1.0f }, 10, 15, null)));
            request.setJsonEntity(Strings.toString(source));
            getRestClient().performRequest(request);
        }

        // search#7 - this will record 1 entry for "query" in `sections`, and 1 for "match_all" under `queries`
        {
            Request request = new Request("GET", INDEX_NAME + "/_search");
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.query(QueryBuilders.matchAllQuery());
            request.setJsonEntity(Strings.toString(source));
            getRestClient().performRequest(request);
        }

        // cluster stats
        {
            SearchUsageStats stats = clusterAdmin().prepareClusterStats().get().getIndicesStats().getSearchUsageStats();
            assertEquals(7, stats.getTotalSearchCount());

            assertThat(stats.getSectionsUsage().size(), equalTo(3));
            assertThat(stats.getSectionsUsage().get("retriever"), equalTo(5L));
            assertThat(stats.getSectionsUsage().get("query"), equalTo(1L));
            assertThat(stats.getSectionsUsage().get("knn"), equalTo(1L));

            assertThat(stats.getRetrieversUsage().size(), equalTo(3));
            assertThat(stats.getRetrieversUsage().get("standard"), equalTo(4L));
            assertThat(stats.getRetrieversUsage().get("knn"), equalTo(1L));
            assertThat(stats.getRetrieversUsage().get("text_similarity_reranker"), equalTo(1L));

            assertThat(stats.getQueryUsage().size(), equalTo(5));
            assertThat(stats.getQueryUsage().get("range"), equalTo(1L));
            assertThat(stats.getQueryUsage().get("term"), equalTo(1L));
            assertThat(stats.getQueryUsage().get("match"), equalTo(1L));
            assertThat(stats.getQueryUsage().get("match_all"), equalTo(1L));
            assertThat(stats.getQueryUsage().get("knn"), equalTo(1L));
        }
    }
}
