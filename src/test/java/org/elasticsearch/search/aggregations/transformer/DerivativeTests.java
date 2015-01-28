/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.transformer;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.search.aggregations.transformer.derivative.Derivative;
import org.elasticsearch.search.aggregations.transformer.derivative.Derivative.GapPolicy;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.derivative;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

@ElasticsearchIntegrationTest.SuiteScopeTest
public class DerivativeTests extends ElasticsearchIntegrationTest {

    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";
    private static final String MULTI_VALUED_FIELD_NAME = "l_values";

    static int numDocs;
    static int interval;
    static int numValueBuckets, numValuesBuckets;
    static int numFirstDerivValueBuckets, numFirstDerivValuesBuckets;
    static long[] valueCounts, valuesCounts;
    static long[] firstDerivValueCounts, firstDerivValuesCounts;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");

        numDocs = randomIntBetween(6, 20);
        interval = randomIntBetween(2, 5);

        numValueBuckets = numDocs / interval + 1;
        valueCounts = new long[numValueBuckets];
        for (int i = 0; i < numDocs; i++) {
            final int bucket = (i + 1) / interval;
            valueCounts[bucket]++;
        }

        numValuesBuckets = (numDocs + 1) / interval + 1;
        valuesCounts = new long[numValuesBuckets];
        for (int i = 0; i < numDocs; i++) {
            final int bucket1 = (i + 1) / interval;
            final int bucket2 = (i + 2) / interval;
            valuesCounts[bucket1]++;
            if (bucket1 != bucket2) {
                valuesCounts[bucket2]++;
            }
        }

        numFirstDerivValueBuckets = numValueBuckets - 1;
        firstDerivValueCounts = new long[numFirstDerivValueBuckets];
        long lastValueCount = -1;
        for (int i = 0; i < numValueBuckets; i++) {
            long thisValue = valueCounts[i];
            if (lastValueCount != -1) {
                long diff = thisValue - lastValueCount;
                firstDerivValueCounts[i - 1] = diff;
            }
            lastValueCount = thisValue;
        }

        numFirstDerivValuesBuckets = numValuesBuckets - 1;
        firstDerivValuesCounts = new long[numFirstDerivValuesBuckets];
        long lastValuesCount = -1;
        for (int i = 0; i < numValuesBuckets; i++) {
            long thisValue = valuesCounts[i];
            if (lastValuesCount != -1) {
                long diff = thisValue - lastValuesCount;
                firstDerivValuesCounts[i - 1] = diff;
            }
            lastValuesCount = thisValue;
        }

        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex("idx", "type").setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, i + 1).startArray(MULTI_VALUED_FIELD_NAME).value(i + 1)
                            .value(i + 2).endArray().field("tag", "tag" + i).endObject()));
        }

        assertAcked(prepareCreate("empty_bucket_idx").addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=integer"));
        builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + 0).setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 0).endObject()));

        builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + 1).setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 1).endObject()));

        builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + 2).setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 2).endObject()));
        builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + 3).setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 2).endObject()));

        builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + 4).setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 4).endObject()));
        builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + 5).setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 4).endObject()));

        builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + 6).setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 5).endObject()));
        builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + 7).setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 5).endObject()));

        builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + 8).setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 9).endObject()));
        builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + 9).setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 9).endObject()));
        builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + 10).setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 9).endObject()));

        builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + 11).setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 10).endObject()));
        builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + 12).setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 10).endObject()));

        builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + 13).setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 11).endObject()));

        indexRandom(true, builders);
        ensureSearchable();
    }

    @Test
    public void singleValuedField() {

        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(derivative("deriv").subAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)))
                .execute().actionGet();

        assertSearchResponse(response);

        Derivative deriv = response.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        List<? extends Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(numFirstDerivValueBuckets));

        for (int i = 0; i < numFirstDerivValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsString(), equalTo(String.valueOf(i * interval)));
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(0l));
            SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
            assertThat(docCountDeriv, notNullValue());
            assertThat(docCountDeriv.value(), equalTo((double) firstDerivValueCounts[i]));
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        derivative("deriv").subAggregation(
                                histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                        .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))).execute().actionGet();

        assertSearchResponse(response);

        Derivative deriv = response.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        assertThat(deriv.getBuckets().size(), equalTo(numFirstDerivValueBuckets));
        Object[] propertiesKeys = (Object[]) deriv.getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) deriv.getProperty("_count");
        Object[] propertiesDocCountDerivs = (Object[]) deriv.getProperty("_doc_count.value");
        Object[] propertiesCounts = (Object[]) deriv.getProperty("sum.value");

        List<Histogram.Bucket> buckets = new ArrayList<>(deriv.getBuckets());
        for (int i = 0; i < numFirstDerivValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsString(), equalTo(String.valueOf(i * interval)));
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(0l));
            assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
            SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
            assertThat(docCountDeriv, notNullValue());
            assertThat(docCountDeriv.value(), equalTo((double) firstDerivValueCounts[i]));
            SimpleValue sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            long s1 = 0;
            long s2 = 0;
            for (int j = 0; j < numDocs; ++j) {
                if ((j + 1) / interval == i) {
                    s1 += j + 1;
                }
                if ((j + 1) / interval == i + 1) {
                    s2 += j + 1;
                }
            }
            long s = s2 - s1;
            assertThat(sum.value(), equalTo((double) s));
            assertThat((long) propertiesKeys[i], equalTo((long) i * interval));
            assertThat((long) propertiesDocCounts[i], equalTo(0l));
            assertThat((double) propertiesDocCountDerivs[i], equalTo((double) firstDerivValueCounts[i]));
            assertThat((double) propertiesCounts[i], equalTo((double) s));
        }
    }

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(derivative("deriv").subAggregation(histogram("histo").field(MULTI_VALUED_FIELD_NAME).interval(interval)))
                .execute().actionGet();

        assertSearchResponse(response);

        Derivative deriv = response.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        List<? extends Bucket> buckets = deriv.getBuckets();
        assertThat(deriv.getBuckets().size(), equalTo(numFirstDerivValuesBuckets));

        for (int i = 0; i < numFirstDerivValuesBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsString(), equalTo(String.valueOf(i * interval)));
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(0l));
            SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
            assertThat(docCountDeriv, notNullValue());
            assertThat(docCountDeriv.value(), equalTo((double) firstDerivValuesCounts[i]));
        }
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(derivative("deriv").subAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)))
                .execute().actionGet();

        assertSearchResponse(response);

        Derivative deriv = response.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        assertThat(deriv.getBuckets().size(), equalTo(0));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(derivative("deriv").subAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)))
                .execute().actionGet();

        assertSearchResponse(response);

        Derivative deriv = response.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        List<? extends Bucket> buckets = deriv.getBuckets();
        assertThat(deriv.getBuckets().size(), equalTo(numFirstDerivValueBuckets));

        for (int i = 0; i < numFirstDerivValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsString(), equalTo(String.valueOf(i * interval)));
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(0l));
            SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
            assertThat(docCountDeriv, notNullValue());
            assertThat(docCountDeriv.value(), equalTo((double) firstDerivValueCounts[i]));
        }
    }

    @Test
    public void singleValuedFieldWithGaps() throws Exception {
        SearchResponse searchResponse = client()
                .prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        derivative("deriv").subAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(1l).minDocCount(0)))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(14l));

        Derivative deriv = searchResponse.getAggregations().get("deriv");
        assertThat(deriv, Matchers.notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        List<Histogram.Bucket> buckets = (List<Bucket>) deriv.getBuckets();
        assertThat(buckets.size(), equalTo(5));

        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(0d));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(1l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(1d));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(0d));

        bucket = buckets.get(3);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(9l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(-1d));

        bucket = buckets.get(4);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(10l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(-1d));
    }

    @Test
    public void singleValuedFieldWithGaps_insertZeros() throws Exception {
        SearchResponse searchResponse = client()
                .prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        derivative("deriv").gapPolicy(GapPolicy.INSERT_ZEROS).subAggregation(
                                histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(1l).minDocCount(0)))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(14l));

        Derivative deriv = searchResponse.getAggregations().get("deriv");
        assertThat(deriv, Matchers.notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        List<Histogram.Bucket> buckets = (List<Bucket>) deriv.getBuckets();
        assertThat(buckets.size(), equalTo(11));

        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(0d));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(1l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(1d));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(2l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(-2d));

        bucket = buckets.get(3);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(3l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(2d));

        bucket = buckets.get(4);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(0d));

        bucket = buckets.get(5);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(5l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(-2d));

        bucket = buckets.get(6);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(6l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(0d));

        bucket = buckets.get(7);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(7l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(0d));

        bucket = buckets.get(8);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(3d));

        bucket = buckets.get(9);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(9l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(-1d));

        bucket = buckets.get(10);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(10l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(-1d));
    }

    @Test
    public void singleValuedFieldWithGaps_interpolate() throws Exception {
        SearchResponse searchResponse = client()
                .prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        derivative("deriv").gapPolicy(GapPolicy.INTERPOLATE).subAggregation(
                                histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(1l).minDocCount(0))).execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(14l));

        Derivative deriv = searchResponse.getAggregations().get("deriv");
        assertThat(deriv, Matchers.notNullValue());
        assertThat(deriv.getName(), equalTo("deriv"));
        List<Histogram.Bucket> buckets = (List<Bucket>) deriv.getBuckets();
        assertThat(buckets.size(), equalTo(7));

        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        SimpleValue docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(0d));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(1l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(1d));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(2l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(0d));

        bucket = buckets.get(3);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(0d));

        bucket = buckets.get(4);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(5l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(0.25d));

        bucket = buckets.get(5);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(9l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(-1d));

        bucket = buckets.get(6);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(10l));
        assertThat(bucket.getDocCount(), equalTo(0l));
        docCountDeriv = bucket.getAggregations().get("_doc_count");
        assertThat(docCountDeriv, notNullValue());
        assertThat(docCountDeriv.value(), equalTo(-1d));
    }

}
