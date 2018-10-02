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

package org.elasticsearch.client;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestConverters.EndpointBuilder;
import org.elasticsearch.client.indexlifecycle.GetLifecyclePolicyRequest;
import org.elasticsearch.client.indexlifecycle.LifecycleManagementStatusRequest;
import org.elasticsearch.client.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.client.indexlifecycle.PutLifecyclePolicyRequest;
import org.elasticsearch.client.indexlifecycle.DeleteLifecyclePolicyRequest;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.rankeval.PrecisionAtK;
import org.elasticsearch.index.rankeval.RankEvalRequest;
import org.elasticsearch.index.rankeval.RankEvalSpec;
import org.elasticsearch.index.rankeval.RatedRequest;
import org.elasticsearch.index.rankeval.RestRankEvalAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.client.indexlifecycle.ExplainLifecycleRequest;
import org.elasticsearch.client.indexlifecycle.SetIndexLifecyclePolicyRequest;
import org.elasticsearch.client.indexlifecycle.StartILMRequest;
import org.elasticsearch.client.indexlifecycle.StopILMRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.MultiSearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.client.RequestConverters.REQUEST_BODY_CONTENT_TYPE;
import static org.elasticsearch.client.RequestConverters.enforceSameContentType;
import static org.elasticsearch.client.indexlifecycle.LifecyclePolicyTests.createRandomPolicy;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.RandomSearchRequestGenerator.randomSearchRequest;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.nullValue;

public class RequestConvertersTests extends ESTestCase {
    public void testPing() {
        Request request = RequestConverters.ping();
        assertEquals("/", request.getEndpoint());
        assertEquals(0, request.getParameters().size());
        assertNull(request.getEntity());
        assertEquals(HttpHead.METHOD_NAME, request.getMethod());
    }

    public void testInfo() {
        Request request = RequestConverters.info();
        assertEquals("/", request.getEndpoint());
        assertEquals(0, request.getParameters().size());
        assertNull(request.getEntity());
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
    }

    public void testGet() {
        getAndExistsTest(RequestConverters::get, HttpGet.METHOD_NAME);
    }

    public void testMultiGet() throws IOException {
        Map<String, String> expectedParams = new HashMap<>();
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        if (randomBoolean()) {
            String preference = randomAlphaOfLength(4);
            multiGetRequest.preference(preference);
            expectedParams.put("preference", preference);
        }
        if (randomBoolean()) {
            multiGetRequest.realtime(randomBoolean());
            if (multiGetRequest.realtime() == false) {
                expectedParams.put("realtime", "false");
            }
        }
        if (randomBoolean()) {
            multiGetRequest.refresh(randomBoolean());
            if (multiGetRequest.refresh()) {
                expectedParams.put("refresh", "true");
            }
        }

        int numberOfRequests = randomIntBetween(0, 32);
        for (int i = 0; i < numberOfRequests; i++) {
            MultiGetRequest.Item item = new MultiGetRequest.Item(randomAlphaOfLength(4), randomAlphaOfLength(4), randomAlphaOfLength(4));
            if (randomBoolean()) {
                item.routing(randomAlphaOfLength(4));
            }
            if (randomBoolean()) {
                item.storedFields(generateRandomStringArray(16, 8, false));
            }
            if (randomBoolean()) {
                item.version(randomNonNegativeLong());
            }
            if (randomBoolean()) {
                item.versionType(randomFrom(VersionType.values()));
            }
            if (randomBoolean()) {
                randomizeFetchSourceContextParams(item::fetchSourceContext, new HashMap<>());
            }
            multiGetRequest.add(item);
        }

        Request request = RequestConverters.multiGet(multiGetRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_mget", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(multiGetRequest, request.getEntity());
    }

    public void testDelete() {
        String index = randomAlphaOfLengthBetween(3, 10);
        String type = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);
        DeleteRequest deleteRequest = new DeleteRequest(index, type, id);

        Map<String, String> expectedParams = new HashMap<>();

        setRandomTimeout(deleteRequest::timeout, ReplicationRequest.DEFAULT_TIMEOUT, expectedParams);
        setRandomRefreshPolicy(deleteRequest::setRefreshPolicy, expectedParams);
        setRandomVersion(deleteRequest, expectedParams);
        setRandomVersionType(deleteRequest::versionType, expectedParams);

        if (frequently()) {
            if (randomBoolean()) {
                String routing = randomAlphaOfLengthBetween(3, 10);
                deleteRequest.routing(routing);
                expectedParams.put("routing", routing);
            }
        }

        Request request = RequestConverters.delete(deleteRequest);
        assertEquals("/" + index + "/" + type + "/" + id, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertNull(request.getEntity());
    }

    public void testExists() {
        getAndExistsTest(RequestConverters::exists, HttpHead.METHOD_NAME);
    }

    private static void getAndExistsTest(Function<GetRequest, Request> requestConverter, String method) {
        String index = randomAlphaOfLengthBetween(3, 10);
        String type = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);
        GetRequest getRequest = new GetRequest(index, type, id);

        Map<String, String> expectedParams = new HashMap<>();
        if (randomBoolean()) {
            if (randomBoolean()) {
                String preference = randomAlphaOfLengthBetween(3, 10);
                getRequest.preference(preference);
                expectedParams.put("preference", preference);
            }
            if (randomBoolean()) {
                String routing = randomAlphaOfLengthBetween(3, 10);
                getRequest.routing(routing);
                expectedParams.put("routing", routing);
            }
            if (randomBoolean()) {
                boolean realtime = randomBoolean();
                getRequest.realtime(realtime);
                if (realtime == false) {
                    expectedParams.put("realtime", "false");
                }
            }
            if (randomBoolean()) {
                boolean refresh = randomBoolean();
                getRequest.refresh(refresh);
                if (refresh) {
                    expectedParams.put("refresh", "true");
                }
            }
            if (randomBoolean()) {
                long version = randomLong();
                getRequest.version(version);
                if (version != Versions.MATCH_ANY) {
                    expectedParams.put("version", Long.toString(version));
                }
            }
            setRandomVersionType(getRequest::versionType, expectedParams);
            if (randomBoolean()) {
                int numStoredFields = randomIntBetween(1, 10);
                String[] storedFields = new String[numStoredFields];
                String storedFieldsParam = randomFields(storedFields);
                getRequest.storedFields(storedFields);
                expectedParams.put("stored_fields", storedFieldsParam);
            }
            if (randomBoolean()) {
                randomizeFetchSourceContextParams(getRequest::fetchSourceContext, expectedParams);
            }
        }
        Request request = requestConverter.apply(getRequest);
        assertEquals("/" + index + "/" + type + "/" + id, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());
        assertEquals(method, request.getMethod());
    }

    public void testReindex() throws IOException {
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices("source_idx");
        reindexRequest.setDestIndex("dest_idx");
        Map<String, String> expectedParams = new HashMap<>();
        if (randomBoolean()) {
            XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
            RemoteInfo remoteInfo = new RemoteInfo("http", "remote-host", 9200, null,
                BytesReference.bytes(matchAllQuery().toXContent(builder, ToXContent.EMPTY_PARAMS)),
                "user",
                "pass",
                emptyMap(),
                RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                RemoteInfo.DEFAULT_CONNECT_TIMEOUT
            );
            reindexRequest.setRemoteInfo(remoteInfo);
        }
        if (randomBoolean()) {
            reindexRequest.setSourceDocTypes("doc", "tweet");
        }
        if (randomBoolean()) {
            reindexRequest.setSourceBatchSize(randomInt(100));
        }
        if (randomBoolean()) {
            reindexRequest.setDestDocType("tweet_and_doc");
        }
        if (randomBoolean()) {
            reindexRequest.setDestOpType("create");
        }
        if (randomBoolean()) {
            reindexRequest.setDestPipeline("my_pipeline");
        }
        if (randomBoolean()) {
            float requestsPerSecond = (float) randomDoubleBetween(0.0, 10.0, false);
            expectedParams.put(RethrottleRequest.REQUEST_PER_SECOND_PARAMETER, Float.toString(requestsPerSecond));
            reindexRequest.setRequestsPerSecond(requestsPerSecond);
        } else {
            expectedParams.put(RethrottleRequest.REQUEST_PER_SECOND_PARAMETER, "-1");
        }
        if (randomBoolean()) {
            reindexRequest.setDestRouting("=cat");
        }
        if (randomBoolean()) {
            reindexRequest.setSize(randomIntBetween(100, 1000));
        }
        if (randomBoolean()) {
            reindexRequest.setAbortOnVersionConflict(false);
        }
        if (randomBoolean()) {
            String ts = randomTimeValue();
            reindexRequest.setScroll(TimeValue.parseTimeValue(ts, "scroll"));
        }
        if (reindexRequest.getRemoteInfo() == null && randomBoolean()) {
            reindexRequest.setSourceQuery(new TermQueryBuilder("foo", "fooval"));
        }
        setRandomTimeout(reindexRequest::setTimeout, ReplicationRequest.DEFAULT_TIMEOUT, expectedParams);
        setRandomWaitForActiveShards(reindexRequest::setWaitForActiveShards, ActiveShardCount.DEFAULT, expectedParams);
        expectedParams.put("scroll", reindexRequest.getScrollTime().getStringRep());
        Request request = RequestConverters.reindex(reindexRequest);
        assertEquals("/_reindex", request.getEndpoint());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(reindexRequest, request.getEntity());
    }

    public void testUpdateByQuery() throws IOException {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(randomIndicesNames(1, 5));
        Map<String, String> expectedParams = new HashMap<>();
        if (randomBoolean()) {
            updateByQueryRequest.setDocTypes(generateRandomStringArray(5, 5, false, false));
        }
        if (randomBoolean()) {
            int batchSize = randomInt(100);
            updateByQueryRequest.setBatchSize(batchSize);
            expectedParams.put("scroll_size", Integer.toString(batchSize));
        }
        if (randomBoolean()) {
            updateByQueryRequest.setPipeline("my_pipeline");
            expectedParams.put("pipeline", "my_pipeline");
        }
        if (randomBoolean()) {
            float requestsPerSecond = (float) randomDoubleBetween(0.0, 10.0, false);
            expectedParams.put("requests_per_second", Float.toString(requestsPerSecond));
            updateByQueryRequest.setRequestsPerSecond(requestsPerSecond);
        } else {
            expectedParams.put("requests_per_second", "-1");
        }
        if (randomBoolean()) {
            updateByQueryRequest.setRouting("=cat");
            expectedParams.put("routing", "=cat");
        }
        if (randomBoolean()) {
            int size = randomIntBetween(100, 1000);
            updateByQueryRequest.setSize(size);
            expectedParams.put("size", Integer.toString(size));
        }
        if (randomBoolean()) {
            updateByQueryRequest.setAbortOnVersionConflict(false);
            expectedParams.put("conflicts", "proceed");
        }
        if (randomBoolean()) {
            String ts = randomTimeValue();
            updateByQueryRequest.setScroll(TimeValue.parseTimeValue(ts, "scroll"));
            expectedParams.put("scroll", ts);
        }
        if (randomBoolean()) {
            updateByQueryRequest.setQuery(new TermQueryBuilder("foo", "fooval"));
        }
        if (randomBoolean()) {
            updateByQueryRequest.setScript(new Script("ctx._source.last = \"lastname\""));
        }
        setRandomIndicesOptions(updateByQueryRequest::setIndicesOptions, updateByQueryRequest::indicesOptions, expectedParams);
        setRandomTimeout(updateByQueryRequest::setTimeout, ReplicationRequest.DEFAULT_TIMEOUT, expectedParams);
        Request request = RequestConverters.updateByQuery(updateByQueryRequest);
        StringJoiner joiner = new StringJoiner("/", "/", "");
        joiner.add(String.join(",", updateByQueryRequest.indices()));
        if (updateByQueryRequest.getDocTypes().length > 0)
            joiner.add(String.join(",", updateByQueryRequest.getDocTypes()));
        joiner.add("_update_by_query");
        assertEquals(joiner.toString(), request.getEndpoint());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(updateByQueryRequest, request.getEntity());
    }

    public void testDeleteByQuery() throws IOException {
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();
        deleteByQueryRequest.indices(randomIndicesNames(1, 5));
        Map<String, String> expectedParams = new HashMap<>();
        if (randomBoolean()) {
            deleteByQueryRequest.setDocTypes(generateRandomStringArray(5, 5, false, false));
        }
        if (randomBoolean()) {
            int batchSize = randomInt(100);
            deleteByQueryRequest.setBatchSize(batchSize);
            expectedParams.put("scroll_size", Integer.toString(batchSize));
        }
        if (randomBoolean()) {
            deleteByQueryRequest.setRouting("=cat");
            expectedParams.put("routing", "=cat");
        }
        if (randomBoolean()) {
            int size = randomIntBetween(100, 1000);
            deleteByQueryRequest.setSize(size);
            expectedParams.put("size", Integer.toString(size));
        }
        if (randomBoolean()) {
            deleteByQueryRequest.setAbortOnVersionConflict(false);
            expectedParams.put("conflicts", "proceed");
        }
        if (randomBoolean()) {
            String ts = randomTimeValue();
            deleteByQueryRequest.setScroll(TimeValue.parseTimeValue(ts, "scroll"));
            expectedParams.put("scroll", ts);
        }
        if (randomBoolean()) {
            deleteByQueryRequest.setQuery(new TermQueryBuilder("foo", "fooval"));
        }
        if (randomBoolean()) {
            float requestsPerSecond = (float) randomDoubleBetween(0.0, 10.0, false);
            expectedParams.put("requests_per_second", Float.toString(requestsPerSecond));
            deleteByQueryRequest.setRequestsPerSecond(requestsPerSecond);
        } else {
            expectedParams.put("requests_per_second", "-1");
        }
        setRandomIndicesOptions(deleteByQueryRequest::setIndicesOptions, deleteByQueryRequest::indicesOptions, expectedParams);
        setRandomTimeout(deleteByQueryRequest::setTimeout, ReplicationRequest.DEFAULT_TIMEOUT, expectedParams);
        Request request = RequestConverters.deleteByQuery(deleteByQueryRequest);
        StringJoiner joiner = new StringJoiner("/", "/", "");
        joiner.add(String.join(",", deleteByQueryRequest.indices()));
        if (deleteByQueryRequest.getDocTypes().length > 0)
            joiner.add(String.join(",", deleteByQueryRequest.getDocTypes()));
        joiner.add("_delete_by_query");
        assertEquals(joiner.toString(), request.getEndpoint());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(deleteByQueryRequest, request.getEntity());
    }

    public void testRethrottle() throws IOException {
        TaskId taskId = new TaskId(randomAlphaOfLength(10), randomIntBetween(1, 100));
        RethrottleRequest rethrottleRequest;
        Float requestsPerSecond;
        Map<String, String> expectedParams = new HashMap<>();
        if (frequently()) {
            requestsPerSecond =  (float) randomDoubleBetween(0.0, 100.0, true);
            rethrottleRequest = new RethrottleRequest(taskId, requestsPerSecond);
            expectedParams.put(RethrottleRequest.REQUEST_PER_SECOND_PARAMETER, Float.toString(requestsPerSecond));
        } else {
            rethrottleRequest = new RethrottleRequest(taskId);
            expectedParams.put(RethrottleRequest.REQUEST_PER_SECOND_PARAMETER, "-1");
        }
        expectedParams.put("group_by", "none");
        Request request = RequestConverters.rethrottle(rethrottleRequest);
        assertEquals("/_reindex/" + taskId + "/_rethrottle", request.getEndpoint());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());

        // test illegal RethrottleRequest values
        Exception e = expectThrows(NullPointerException.class, () -> new RethrottleRequest(null, 1.0f));
        assertEquals("taskId cannot be null", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new RethrottleRequest(new TaskId("taskId", 1), -5.0f));
        assertEquals("requestsPerSecond needs to be positive value but was [-5.0]", e.getMessage());
    }

    public void testIndex() throws IOException {
        String index = randomAlphaOfLengthBetween(3, 10);
        String type = randomAlphaOfLengthBetween(3, 10);
        IndexRequest indexRequest = new IndexRequest(index, type);

        String id = randomBoolean() ? randomAlphaOfLengthBetween(3, 10) : null;
        indexRequest.id(id);

        Map<String, String> expectedParams = new HashMap<>();

        String method = HttpPost.METHOD_NAME;
        if (id != null) {
            method = HttpPut.METHOD_NAME;
            if (randomBoolean()) {
                indexRequest.opType(DocWriteRequest.OpType.CREATE);
            }
        }

        setRandomTimeout(indexRequest::timeout, ReplicationRequest.DEFAULT_TIMEOUT, expectedParams);
        setRandomRefreshPolicy(indexRequest::setRefreshPolicy, expectedParams);

        // There is some logic around _create endpoint and version/version type
        if (indexRequest.opType() == DocWriteRequest.OpType.CREATE) {
            indexRequest.version(randomFrom(Versions.MATCH_ANY, Versions.MATCH_DELETED));
            expectedParams.put("version", Long.toString(Versions.MATCH_DELETED));
        } else {
            setRandomVersion(indexRequest, expectedParams);
            setRandomVersionType(indexRequest::versionType, expectedParams);
        }

        if (frequently()) {
            if (randomBoolean()) {
                String routing = randomAlphaOfLengthBetween(3, 10);
                indexRequest.routing(routing);
                expectedParams.put("routing", routing);
            }
            if (randomBoolean()) {
                String pipeline = randomAlphaOfLengthBetween(3, 10);
                indexRequest.setPipeline(pipeline);
                expectedParams.put("pipeline", pipeline);
            }
        }

        XContentType xContentType = randomFrom(XContentType.values());
        int nbFields = randomIntBetween(0, 10);
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();
            for (int i = 0; i < nbFields; i++) {
                builder.field("field_" + i, i);
            }
            builder.endObject();
            indexRequest.source(builder);
        }

        Request request = RequestConverters.index(indexRequest);
        if (indexRequest.opType() == DocWriteRequest.OpType.CREATE) {
            assertEquals("/" + index + "/" + type + "/" + id + "/_create", request.getEndpoint());
        } else if (id != null) {
            assertEquals("/" + index + "/" + type + "/" + id, request.getEndpoint());
        } else {
            assertEquals("/" + index + "/" + type, request.getEndpoint());
        }
        assertEquals(expectedParams, request.getParameters());
        assertEquals(method, request.getMethod());

        HttpEntity entity = request.getEntity();
        assertTrue(entity instanceof ByteArrayEntity);
        assertEquals(indexRequest.getContentType().mediaTypeWithoutParameters(), entity.getContentType().getValue());
        try (XContentParser parser = createParser(xContentType.xContent(), entity.getContent())) {
            assertEquals(nbFields, parser.map().size());
        }
    }

    public void testUpdate() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());

        Map<String, String> expectedParams = new HashMap<>();
        String index = randomAlphaOfLengthBetween(3, 10);
        String type = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);

        UpdateRequest updateRequest = new UpdateRequest(index, type, id);
        updateRequest.detectNoop(randomBoolean());

        if (randomBoolean()) {
            BytesReference source = RandomObjects.randomSource(random(), xContentType);
            updateRequest.doc(new IndexRequest().source(source, xContentType));

            boolean docAsUpsert = randomBoolean();
            updateRequest.docAsUpsert(docAsUpsert);
            if (docAsUpsert) {
                expectedParams.put("doc_as_upsert", "true");
            }
        } else {
            updateRequest.script(mockScript("_value + 1"));
            updateRequest.scriptedUpsert(randomBoolean());
        }
        if (randomBoolean()) {
            BytesReference source = RandomObjects.randomSource(random(), xContentType);
            updateRequest.upsert(new IndexRequest().source(source, xContentType));
        }
        if (randomBoolean()) {
            String routing = randomAlphaOfLengthBetween(3, 10);
            updateRequest.routing(routing);
            expectedParams.put("routing", routing);
        }
        if (randomBoolean()) {
            String timeout = randomTimeValue();
            updateRequest.timeout(timeout);
            expectedParams.put("timeout", timeout);
        } else {
            expectedParams.put("timeout", ReplicationRequest.DEFAULT_TIMEOUT.getStringRep());
        }
        if (randomBoolean()) {
            WriteRequest.RefreshPolicy refreshPolicy = randomFrom(WriteRequest.RefreshPolicy.values());
            updateRequest.setRefreshPolicy(refreshPolicy);
            if (refreshPolicy != WriteRequest.RefreshPolicy.NONE) {
                expectedParams.put("refresh", refreshPolicy.getValue());
            }
        }
        setRandomWaitForActiveShards(updateRequest::waitForActiveShards, expectedParams);
        setRandomVersion(updateRequest, expectedParams);
        setRandomVersionType(updateRequest::versionType, expectedParams);
        if (randomBoolean()) {
            int retryOnConflict = randomIntBetween(0, 5);
            updateRequest.retryOnConflict(retryOnConflict);
            if (retryOnConflict > 0) {
                expectedParams.put("retry_on_conflict", String.valueOf(retryOnConflict));
            }
        }
        if (randomBoolean()) {
            randomizeFetchSourceContextParams(updateRequest::fetchSource, expectedParams);
        }

        Request request = RequestConverters.update(updateRequest);
        assertEquals("/" + index + "/" + type + "/" + id + "/_update", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());

        HttpEntity entity = request.getEntity();
        assertTrue(entity instanceof ByteArrayEntity);

        UpdateRequest parsedUpdateRequest = new UpdateRequest();

        XContentType entityContentType = XContentType.fromMediaTypeOrFormat(entity.getContentType().getValue());
        try (XContentParser parser = createParser(entityContentType.xContent(), entity.getContent())) {
            parsedUpdateRequest.fromXContent(parser);
        }

        assertEquals(updateRequest.scriptedUpsert(), parsedUpdateRequest.scriptedUpsert());
        assertEquals(updateRequest.docAsUpsert(), parsedUpdateRequest.docAsUpsert());
        assertEquals(updateRequest.detectNoop(), parsedUpdateRequest.detectNoop());
        assertEquals(updateRequest.fetchSource(), parsedUpdateRequest.fetchSource());
        assertEquals(updateRequest.script(), parsedUpdateRequest.script());
        if (updateRequest.doc() != null) {
            assertToXContentEquivalent(updateRequest.doc().source(), parsedUpdateRequest.doc().source(), xContentType);
        } else {
            assertNull(parsedUpdateRequest.doc());
        }
        if (updateRequest.upsertRequest() != null) {
            assertToXContentEquivalent(updateRequest.upsertRequest().source(), parsedUpdateRequest.upsertRequest().source(), xContentType);
        } else {
            assertNull(parsedUpdateRequest.upsertRequest());
        }
    }

    public void testUpdateWithDifferentContentTypes() {
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.doc(new IndexRequest().source(singletonMap("field", "doc"), XContentType.JSON));
            updateRequest.upsert(new IndexRequest().source(singletonMap("field", "upsert"), XContentType.YAML));
            RequestConverters.update(updateRequest);
        });
        assertEquals("Update request cannot have different content types for doc [JSON] and upsert [YAML] documents",
                exception.getMessage());
    }

    public void testBulk() throws IOException {
        Map<String, String> expectedParams = new HashMap<>();

        BulkRequest bulkRequest = new BulkRequest();
        if (randomBoolean()) {
            String timeout = randomTimeValue();
            bulkRequest.timeout(timeout);
            expectedParams.put("timeout", timeout);
        } else {
            expectedParams.put("timeout", BulkShardRequest.DEFAULT_TIMEOUT.getStringRep());
        }

        setRandomRefreshPolicy(bulkRequest::setRefreshPolicy, expectedParams);

        XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);

        int nbItems = randomIntBetween(10, 100);
        for (int i = 0; i < nbItems; i++) {
            String index = randomAlphaOfLength(5);
            String type = randomAlphaOfLength(5);
            String id = randomAlphaOfLength(5);

            BytesReference source = RandomObjects.randomSource(random(), xContentType);
            DocWriteRequest.OpType opType = randomFrom(DocWriteRequest.OpType.values());

            DocWriteRequest<?> docWriteRequest;
            if (opType == DocWriteRequest.OpType.INDEX) {
                IndexRequest indexRequest = new IndexRequest(index, type, id).source(source, xContentType);
                docWriteRequest = indexRequest;
                if (randomBoolean()) {
                    indexRequest.setPipeline(randomAlphaOfLength(5));
                }
            } else if (opType == DocWriteRequest.OpType.CREATE) {
                IndexRequest createRequest = new IndexRequest(index, type, id).source(source, xContentType).create(true);
                docWriteRequest = createRequest;
            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                final UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(new IndexRequest().source(source, xContentType));
                docWriteRequest = updateRequest;
                if (randomBoolean()) {
                    updateRequest.retryOnConflict(randomIntBetween(1, 5));
                }
                if (randomBoolean()) {
                    randomizeFetchSourceContextParams(updateRequest::fetchSource, new HashMap<>());
                }
            } else if (opType == DocWriteRequest.OpType.DELETE) {
                docWriteRequest = new DeleteRequest(index, type, id);
            } else {
                throw new UnsupportedOperationException("optype [" + opType + "] not supported");
            }

            if (randomBoolean()) {
                docWriteRequest.routing(randomAlphaOfLength(10));
            }
            if (randomBoolean()) {
                docWriteRequest.version(randomNonNegativeLong());
            }
            if (randomBoolean()) {
                docWriteRequest.versionType(randomFrom(VersionType.values()));
            }
            bulkRequest.add(docWriteRequest);
        }

        Request request = RequestConverters.bulk(bulkRequest);
        assertEquals("/_bulk", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(xContentType.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
        byte[] content = new byte[(int) request.getEntity().getContentLength()];
        try (InputStream inputStream = request.getEntity().getContent()) {
            Streams.readFully(inputStream, content);
        }

        BulkRequest parsedBulkRequest = new BulkRequest();
        parsedBulkRequest.add(content, 0, content.length, xContentType);
        assertEquals(bulkRequest.numberOfActions(), parsedBulkRequest.numberOfActions());

        for (int i = 0; i < bulkRequest.numberOfActions(); i++) {
            DocWriteRequest<?> originalRequest = bulkRequest.requests().get(i);
            DocWriteRequest<?> parsedRequest = parsedBulkRequest.requests().get(i);

            assertEquals(originalRequest.opType(), parsedRequest.opType());
            assertEquals(originalRequest.index(), parsedRequest.index());
            assertEquals(originalRequest.type(), parsedRequest.type());
            assertEquals(originalRequest.id(), parsedRequest.id());
            assertEquals(originalRequest.routing(), parsedRequest.routing());
            assertEquals(originalRequest.version(), parsedRequest.version());
            assertEquals(originalRequest.versionType(), parsedRequest.versionType());

            DocWriteRequest.OpType opType = originalRequest.opType();
            if (opType == DocWriteRequest.OpType.INDEX) {
                IndexRequest indexRequest = (IndexRequest) originalRequest;
                IndexRequest parsedIndexRequest = (IndexRequest) parsedRequest;

                assertEquals(indexRequest.getPipeline(), parsedIndexRequest.getPipeline());
                assertToXContentEquivalent(indexRequest.source(), parsedIndexRequest.source(), xContentType);
            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                UpdateRequest updateRequest = (UpdateRequest) originalRequest;
                UpdateRequest parsedUpdateRequest = (UpdateRequest) parsedRequest;

                assertEquals(updateRequest.retryOnConflict(), parsedUpdateRequest.retryOnConflict());
                assertEquals(updateRequest.fetchSource(), parsedUpdateRequest.fetchSource());
                if (updateRequest.doc() != null) {
                    assertToXContentEquivalent(updateRequest.doc().source(), parsedUpdateRequest.doc().source(), xContentType);
                } else {
                    assertNull(parsedUpdateRequest.doc());
                }
            }
        }
    }

    public void testBulkWithDifferentContentTypes() throws IOException {
        {
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new DeleteRequest("index", "type", "0"));
            bulkRequest.add(new UpdateRequest("index", "type", "1").script(mockScript("test")));
            bulkRequest.add(new DeleteRequest("index", "type", "2"));

            Request request = RequestConverters.bulk(bulkRequest);
            assertEquals(XContentType.JSON.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
        }
        {
            XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new DeleteRequest("index", "type", "0"));
            bulkRequest.add(new IndexRequest("index", "type", "0").source(singletonMap("field", "value"), xContentType));
            bulkRequest.add(new DeleteRequest("index", "type", "2"));

            Request request = RequestConverters.bulk(bulkRequest);
            assertEquals(xContentType.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
        }
        {
            XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);
            UpdateRequest updateRequest = new UpdateRequest("index", "type", "0");
            if (randomBoolean()) {
                updateRequest.doc(new IndexRequest().source(singletonMap("field", "value"), xContentType));
            } else {
                updateRequest.upsert(new IndexRequest().source(singletonMap("field", "value"), xContentType));
            }

            Request request = RequestConverters.bulk(new BulkRequest().add(updateRequest));
            assertEquals(xContentType.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
        }
        {
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new IndexRequest("index", "type", "0").source(singletonMap("field", "value"), XContentType.SMILE));
            bulkRequest.add(new IndexRequest("index", "type", "1").source(singletonMap("field", "value"), XContentType.JSON));
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> RequestConverters.bulk(bulkRequest));
            assertEquals(
                    "Mismatching content-type found for request with content-type [JSON], " + "previous requests have content-type [SMILE]",
                    exception.getMessage());
        }
        {
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new IndexRequest("index", "type", "0").source(singletonMap("field", "value"), XContentType.JSON));
            bulkRequest.add(new IndexRequest("index", "type", "1").source(singletonMap("field", "value"), XContentType.JSON));
            bulkRequest.add(new UpdateRequest("index", "type", "2")
                    .doc(new IndexRequest().source(singletonMap("field", "value"), XContentType.JSON))
                    .upsert(new IndexRequest().source(singletonMap("field", "value"), XContentType.SMILE)));
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> RequestConverters.bulk(bulkRequest));
            assertEquals(
                    "Mismatching content-type found for request with content-type [SMILE], " + "previous requests have content-type [JSON]",
                    exception.getMessage());
        }
        {
            XContentType xContentType = randomFrom(XContentType.CBOR, XContentType.YAML);
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new DeleteRequest("index", "type", "0"));
            bulkRequest.add(new IndexRequest("index", "type", "1").source(singletonMap("field", "value"), XContentType.JSON));
            bulkRequest.add(new DeleteRequest("index", "type", "2"));
            bulkRequest.add(new DeleteRequest("index", "type", "3"));
            bulkRequest.add(new IndexRequest("index", "type", "4").source(singletonMap("field", "value"), XContentType.JSON));
            bulkRequest.add(new IndexRequest("index", "type", "1").source(singletonMap("field", "value"), xContentType));
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> RequestConverters.bulk(bulkRequest));
            assertEquals("Unsupported content-type found for request with content-type [" + xContentType
                    + "], only JSON and SMILE are supported", exception.getMessage());
        }
    }

    public void testSearchNullSource() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        Request request = RequestConverters.search(searchRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_search", request.getEndpoint());
        assertNull(request.getEntity());
    }

    public void testSearch() throws Exception {
        String[] indices = randomIndicesNames(0, 5);
        SearchRequest searchRequest = new SearchRequest(indices);

        int numTypes = randomIntBetween(0, 5);
        String[] types = new String[numTypes];
        for (int i = 0; i < numTypes; i++) {
            types[i] = "type-" + randomAlphaOfLengthBetween(2, 5);
        }
        searchRequest.types(types);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomSearchParams(searchRequest, expectedParams);
        setRandomIndicesOptions(searchRequest::indicesOptions, searchRequest::indicesOptions, expectedParams);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // rarely skip setting the search source completely
        if (frequently()) {
            // frequently set the search source to have some content, otherwise leave it
            // empty but still set it
            if (frequently()) {
                if (randomBoolean()) {
                    searchSourceBuilder.size(randomIntBetween(0, Integer.MAX_VALUE));
                }
                if (randomBoolean()) {
                    searchSourceBuilder.from(randomIntBetween(0, Integer.MAX_VALUE));
                }
                if (randomBoolean()) {
                    searchSourceBuilder.minScore(randomFloat());
                }
                if (randomBoolean()) {
                    searchSourceBuilder.explain(randomBoolean());
                }
                if (randomBoolean()) {
                    searchSourceBuilder.profile(randomBoolean());
                }
                if (randomBoolean()) {
                    searchSourceBuilder.highlighter(new HighlightBuilder().field(randomAlphaOfLengthBetween(3, 10)));
                }
                if (randomBoolean()) {
                    searchSourceBuilder.query(new TermQueryBuilder(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10)));
                }
                if (randomBoolean()) {
                    searchSourceBuilder.aggregation(new TermsAggregationBuilder(randomAlphaOfLengthBetween(3, 10), ValueType.STRING)
                            .field(randomAlphaOfLengthBetween(3, 10)));
                }
                if (randomBoolean()) {
                    searchSourceBuilder.suggest(new SuggestBuilder().addSuggestion(randomAlphaOfLengthBetween(3, 10),
                            new CompletionSuggestionBuilder(randomAlphaOfLengthBetween(3, 10))));
                }
                if (randomBoolean()) {
                    searchSourceBuilder.addRescorer(new QueryRescorerBuilder(
                            new TermQueryBuilder(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10))));
                }
                if (randomBoolean()) {
                    searchSourceBuilder.collapse(new CollapseBuilder(randomAlphaOfLengthBetween(3, 10)));
                }
            }
            searchRequest.source(searchSourceBuilder);
        }

        Request request = RequestConverters.search(searchRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        String type = String.join(",", types);
        if (Strings.hasLength(type)) {
            endpoint.add(type);
        }
        endpoint.add("_search");
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(endpoint.toString(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(searchSourceBuilder, request.getEntity());
    }

    public void testSearchNullIndicesAndTypes() {
        expectThrows(NullPointerException.class, () -> new SearchRequest((String[]) null));
        expectThrows(NullPointerException.class, () -> new SearchRequest().indices((String[]) null));
        expectThrows(NullPointerException.class, () -> new SearchRequest().types((String[]) null));
    }

    public void testMultiSearch() throws IOException {
        int numberOfSearchRequests = randomIntBetween(0, 32);
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (int i = 0; i < numberOfSearchRequests; i++) {
            SearchRequest searchRequest = randomSearchRequest(() -> {
                // No need to return a very complex SearchSourceBuilder here, that is tested
                // elsewhere
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.from(randomInt(10));
                searchSourceBuilder.size(randomIntBetween(20, 100));
                return searchSourceBuilder;
            });
            // scroll is not supported in the current msearch api, so unset it:
            searchRequest.scroll((Scroll) null);
            // only expand_wildcards, ignore_unavailable and allow_no_indices can be
            // specified from msearch api, so unset other options:
            IndicesOptions randomlyGenerated = searchRequest.indicesOptions();
            IndicesOptions msearchDefault = new MultiSearchRequest().indicesOptions();
            searchRequest.indicesOptions(IndicesOptions.fromOptions(randomlyGenerated.ignoreUnavailable(),
                    randomlyGenerated.allowNoIndices(), randomlyGenerated.expandWildcardsOpen(), randomlyGenerated.expandWildcardsClosed(),
                    msearchDefault.allowAliasesToMultipleIndices(), msearchDefault.forbidClosedIndices(), msearchDefault.ignoreAliases()));
            multiSearchRequest.add(searchRequest);
        }

        Map<String, String> expectedParams = new HashMap<>();
        expectedParams.put(RestSearchAction.TYPED_KEYS_PARAM, "true");
        if (randomBoolean()) {
            multiSearchRequest.maxConcurrentSearchRequests(randomIntBetween(1, 8));
            expectedParams.put("max_concurrent_searches", Integer.toString(multiSearchRequest.maxConcurrentSearchRequests()));
        }

        Request request = RequestConverters.multiSearch(multiSearchRequest);
        assertEquals("/_msearch", request.getEndpoint());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(expectedParams, request.getParameters());

        List<SearchRequest> requests = new ArrayList<>();
        CheckedBiConsumer<SearchRequest, XContentParser, IOException> consumer = (searchRequest, p) -> {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(p, false);
            if (searchSourceBuilder.equals(new SearchSourceBuilder()) == false) {
                searchRequest.source(searchSourceBuilder);
            }
            requests.add(searchRequest);
        };
        MultiSearchRequest.readMultiLineFormat(new BytesArray(EntityUtils.toByteArray(request.getEntity())),
                REQUEST_BODY_CONTENT_TYPE.xContent(), consumer, null, multiSearchRequest.indicesOptions(), null, null, null,
                xContentRegistry(), true);
        assertEquals(requests, multiSearchRequest.requests());
    }

    public void testSearchScroll() throws IOException {
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        searchScrollRequest.scrollId(randomAlphaOfLengthBetween(5, 10));
        if (randomBoolean()) {
            searchScrollRequest.scroll(randomPositiveTimeValue());
        }
        Request request = RequestConverters.searchScroll(searchScrollRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_search/scroll", request.getEndpoint());
        assertEquals(0, request.getParameters().size());
        assertToXContentBody(searchScrollRequest, request.getEntity());
        assertEquals(REQUEST_BODY_CONTENT_TYPE.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
    }

    public void testClearScroll() throws IOException {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        int numScrolls = randomIntBetween(1, 10);
        for (int i = 0; i < numScrolls; i++) {
            clearScrollRequest.addScrollId(randomAlphaOfLengthBetween(5, 10));
        }
        Request request = RequestConverters.clearScroll(clearScrollRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_search/scroll", request.getEndpoint());
        assertEquals(0, request.getParameters().size());
        assertToXContentBody(clearScrollRequest, request.getEntity());
        assertEquals(REQUEST_BODY_CONTENT_TYPE.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
    }

    public void testSearchTemplate() throws Exception {
        // Create a random request.
        String[] indices = randomIndicesNames(0, 5);
        SearchRequest searchRequest = new SearchRequest(indices);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomSearchParams(searchRequest, expectedParams);
        setRandomIndicesOptions(searchRequest::indicesOptions, searchRequest::indicesOptions, expectedParams);

        SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest(searchRequest);

        searchTemplateRequest.setScript("{\"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" }}}");
        searchTemplateRequest.setScriptType(ScriptType.INLINE);
        searchTemplateRequest.setProfile(randomBoolean());

        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("field", "name");
        scriptParams.put("value", "soren");
        searchTemplateRequest.setScriptParams(scriptParams);

        // Verify that the resulting REST request looks as expected.
        Request request = RequestConverters.searchTemplate(searchTemplateRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_search/template");

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals(endpoint.toString(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(searchTemplateRequest, request.getEntity());
    }

    public void testRenderSearchTemplate() throws Exception {
        // Create a simple request.
        SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest();
        searchTemplateRequest.setSimulate(true); // Setting simulate true means the template should only be rendered.

        searchTemplateRequest.setScript("template1");
        searchTemplateRequest.setScriptType(ScriptType.STORED);
        searchTemplateRequest.setProfile(randomBoolean());

        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("field", "name");
        scriptParams.put("value", "soren");
        searchTemplateRequest.setScriptParams(scriptParams);

        // Verify that the resulting REST request looks as expected.
        Request request = RequestConverters.searchTemplate(searchTemplateRequest);
        String endpoint = "_render/template";

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals(endpoint, request.getEndpoint());
        assertEquals(Collections.emptyMap(), request.getParameters());
        assertToXContentBody(searchTemplateRequest, request.getEntity());
    }

    public void testMultiSearchTemplate() throws Exception {
        final int numSearchRequests = randomIntBetween(1, 10);
        MultiSearchTemplateRequest multiSearchTemplateRequest = new MultiSearchTemplateRequest();

        for (int i = 0; i < numSearchRequests; i++) {
            // Create a random request.
            String[] indices = randomIndicesNames(0, 5);
            SearchRequest searchRequest = new SearchRequest(indices);

            Map<String, String> expectedParams = new HashMap<>();
            setRandomSearchParams(searchRequest, expectedParams);

            // scroll is not supported in the current msearch or msearchtemplate api, so unset it:
            searchRequest.scroll((Scroll) null);
            // batched reduce size is currently not set-able on a per-request basis as it is a query string parameter only
            searchRequest.setBatchedReduceSize(SearchRequest.DEFAULT_BATCHED_REDUCE_SIZE);

            setRandomIndicesOptions(searchRequest::indicesOptions, searchRequest::indicesOptions, expectedParams);

            SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest(searchRequest);

            searchTemplateRequest.setScript("{\"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" }}}");
            searchTemplateRequest.setScriptType(ScriptType.INLINE);
            searchTemplateRequest.setProfile(randomBoolean());

            Map<String, Object> scriptParams = new HashMap<>();
            scriptParams.put("field", "name");
            scriptParams.put("value", randomAlphaOfLengthBetween(2, 5));
            searchTemplateRequest.setScriptParams(scriptParams);

            multiSearchTemplateRequest.add(searchTemplateRequest);
        }

        Request multiRequest = RequestConverters.multiSearchTemplate(multiSearchTemplateRequest);

        assertEquals(HttpPost.METHOD_NAME, multiRequest.getMethod());
        assertEquals("/_msearch/template", multiRequest.getEndpoint());
        List<SearchTemplateRequest> searchRequests = multiSearchTemplateRequest.requests();
        assertEquals(numSearchRequests, searchRequests.size());

        HttpEntity actualEntity = multiRequest.getEntity();
        byte[] expectedBytes = MultiSearchTemplateRequest.writeMultiLineFormat(multiSearchTemplateRequest, XContentType.JSON.xContent());
        assertEquals(XContentType.JSON.mediaTypeWithoutParameters(), actualEntity.getContentType().getValue());
        assertEquals(new BytesArray(expectedBytes), new BytesArray(EntityUtils.toByteArray(actualEntity)));
    }

    public void testExplain() throws IOException {
        String index = randomAlphaOfLengthBetween(3, 10);
        String type = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);

        ExplainRequest explainRequest = new ExplainRequest(index, type, id);
        explainRequest.query(QueryBuilders.termQuery(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10)));

        Map<String, String> expectedParams = new HashMap<>();

        if (randomBoolean()) {
            String routing = randomAlphaOfLengthBetween(3, 10);
            explainRequest.routing(routing);
            expectedParams.put("routing", routing);
        }
        if (randomBoolean()) {
            String preference = randomAlphaOfLengthBetween(3, 10);
            explainRequest.preference(preference);
            expectedParams.put("preference", preference);
        }
        if (randomBoolean()) {
            String[] storedFields = generateRandomStringArray(10, 5, false, false);
            String storedFieldsParams = randomFields(storedFields);
            explainRequest.storedFields(storedFields);
            expectedParams.put("stored_fields", storedFieldsParams);
        }
        if (randomBoolean()) {
            randomizeFetchSourceContextParams(explainRequest::fetchSourceContext, expectedParams);
        }

        Request request = RequestConverters.explain(explainRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        endpoint.add(index)
            .add(type)
            .add(id)
            .add("_explain");

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals(endpoint.toString(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(explainRequest, request.getEntity());
    }

    public void testFieldCaps() {
        // Create a random request.
        String[] indices = randomIndicesNames(0, 5);
        String[] fields = generateRandomStringArray(5, 10, false, false);

        FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest().indices(indices).fields(fields);

        Map<String, String> indicesOptionsParams = new HashMap<>();
        setRandomIndicesOptions(fieldCapabilitiesRequest::indicesOptions, fieldCapabilitiesRequest::indicesOptions, indicesOptionsParams);

        Request request = RequestConverters.fieldCaps(fieldCapabilitiesRequest);

        // Verify that the resulting REST request looks as expected.
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String joinedIndices = String.join(",", indices);
        if (!joinedIndices.isEmpty()) {
            endpoint.add(joinedIndices);
        }
        endpoint.add("_field_caps");

        assertEquals(endpoint.toString(), request.getEndpoint());
        assertEquals(4, request.getParameters().size());

        // Note that we don't check the field param value explicitly, as field names are
        // passed through
        // a hash set before being added to the request, and can appear in a
        // non-deterministic order.
        assertThat(request.getParameters(), hasKey("fields"));
        String[] requestFields = Strings.splitStringByCommaToArray(request.getParameters().get("fields"));
        assertEquals(new HashSet<>(Arrays.asList(fields)), new HashSet<>(Arrays.asList(requestFields)));

        for (Map.Entry<String, String> param : indicesOptionsParams.entrySet()) {
            assertThat(request.getParameters(), hasEntry(param.getKey(), param.getValue()));
        }

        assertNull(request.getEntity());
    }

    public void testRankEval() throws Exception {
        RankEvalSpec spec = new RankEvalSpec(
                Collections.singletonList(new RatedRequest("queryId", Collections.emptyList(), new SearchSourceBuilder())),
                new PrecisionAtK());
        String[] indices = randomIndicesNames(0, 5);
        RankEvalRequest rankEvalRequest = new RankEvalRequest(spec, indices);
        Map<String, String> expectedParams = new HashMap<>();
        setRandomIndicesOptions(rankEvalRequest::indicesOptions, rankEvalRequest::indicesOptions, expectedParams);

        Request request = RequestConverters.rankEval(rankEvalRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add(RestRankEvalAction.ENDPOINT);
        assertEquals(endpoint.toString(), request.getEndpoint());
        assertEquals(3, request.getParameters().size());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(spec, request.getEntity());
    }

    public void testPutScript() throws Exception {
        PutStoredScriptRequest putStoredScriptRequest = new PutStoredScriptRequest();

        String id = randomAlphaOfLengthBetween(5, 10);
        putStoredScriptRequest.id(id);

        XContentType xContentType = randomFrom(XContentType.values());
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();
            builder.startObject("script")
                .field("lang", "painless")
                .field("source", "Math.log(_score * 2) + params.multiplier")
                .endObject();
            builder.endObject();

            putStoredScriptRequest.content(BytesReference.bytes(builder), xContentType);
        }

        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(putStoredScriptRequest, expectedParams);
        setRandomTimeout(putStoredScriptRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);

        if (randomBoolean()) {
            String context = randomAlphaOfLengthBetween(5, 10);
            putStoredScriptRequest.context(context);
            expectedParams.put("context", context);
        }

        Request request = RequestConverters.putScript(putStoredScriptRequest);

        assertThat(request.getEndpoint(), equalTo("/_scripts/" + id));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertNotNull(request.getEntity());
        assertToXContentBody(putStoredScriptRequest, request.getEntity());
    }

    public void testAnalyzeRequest() throws Exception {
        AnalyzeRequest indexAnalyzeRequest = new AnalyzeRequest()
            .text("Here is some text")
            .index("test_index")
            .analyzer("test_analyzer");

        Request request = RequestConverters.analyze(indexAnalyzeRequest);
        assertThat(request.getEndpoint(), equalTo("/test_index/_analyze"));
        assertToXContentBody(indexAnalyzeRequest, request.getEntity());

        AnalyzeRequest analyzeRequest = new AnalyzeRequest()
            .text("more text")
            .analyzer("test_analyzer");
        assertThat(RequestConverters.analyze(analyzeRequest).getEndpoint(), equalTo("/_analyze"));
    }

    public void testGetScriptRequest() {
        GetStoredScriptRequest getStoredScriptRequest = new GetStoredScriptRequest("x-script");
        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(getStoredScriptRequest, expectedParams);

        Request request = RequestConverters.getScript(getStoredScriptRequest);
        assertThat(request.getEndpoint(), equalTo("/_scripts/" + getStoredScriptRequest.id()));
        assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());
    }

    public void testDeleteScriptRequest() {
        DeleteStoredScriptRequest deleteStoredScriptRequest = new DeleteStoredScriptRequest("x-script");

        Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(deleteStoredScriptRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(deleteStoredScriptRequest, expectedParams);

        Request request = RequestConverters.deleteScript(deleteStoredScriptRequest);
        assertThat(request.getEndpoint(), equalTo("/_scripts/" + deleteStoredScriptRequest.id()));
        assertThat(request.getMethod(), equalTo(HttpDelete.METHOD_NAME));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());
    }

    static void assertToXContentBody(ToXContent expectedBody, HttpEntity actualEntity) throws IOException {
        BytesReference expectedBytes = XContentHelper.toXContent(expectedBody, REQUEST_BODY_CONTENT_TYPE, false);
        assertEquals(XContentType.JSON.mediaTypeWithoutParameters(), actualEntity.getContentType().getValue());
        assertEquals(expectedBytes, new BytesArray(EntityUtils.toByteArray(actualEntity)));
    }

    public void testEndpointBuilder() {
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder();
            assertEquals("/", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart(Strings.EMPTY_ARRAY);
            assertEquals("/", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("");
            assertEquals("/", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("a", "b");
            assertEquals("/a/b", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("a").addPathPart("b").addPathPartAsIs("_create");
            assertEquals("/a/b/_create", endpointBuilder.build());
        }

        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("a", "b", "c").addPathPartAsIs("_create");
            assertEquals("/a/b/c/_create", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("a").addPathPartAsIs("_create");
            assertEquals("/a/_create", endpointBuilder.build());
        }
    }

    public void testEndpointBuilderEncodeParts() {
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("-#index1,index#2", "type", "id");
            assertEquals("/-%23index1,index%232/type/id", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("index", "type#2", "id");
            assertEquals("/index/type%232/id", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("index", "type", "this/is/the/id");
            assertEquals("/index/type/this%2Fis%2Fthe%2Fid", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("index", "type", "this|is|the|id");
            assertEquals("/index/type/this%7Cis%7Cthe%7Cid", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("index", "type", "id#1");
            assertEquals("/index/type/id%231", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("<logstash-{now/M}>", "_search");
            assertEquals("/%3Clogstash-%7Bnow%2FM%7D%3E/_search", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("中文");
            assertEquals("/中文", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("foo bar");
            assertEquals("/foo%20bar", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("foo+bar");
            assertEquals("/foo+bar", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("foo+bar");
            assertEquals("/foo+bar", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("foo/bar");
            assertEquals("/foo%2Fbar", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("foo^bar");
            assertEquals("/foo%5Ebar", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("cluster1:index1,index2").addPathPartAsIs("_search");
            assertEquals("/cluster1:index1,index2/_search", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addCommaSeparatedPathParts(new String[] { "index1", "index2" })
                    .addPathPartAsIs("cache/clear");
            assertEquals("/index1,index2/cache/clear", endpointBuilder.build());
        }
    }

    public void testEndpoint() {
        assertEquals("/index/type/id", RequestConverters.endpoint("index", "type", "id"));
        assertEquals("/index/type/id/_endpoint", RequestConverters.endpoint("index", "type", "id", "_endpoint"));
        assertEquals("/index1,index2", RequestConverters.endpoint(new String[] { "index1", "index2" }));
        assertEquals("/index1,index2/_endpoint", RequestConverters.endpoint(new String[] { "index1", "index2" }, "_endpoint"));
        assertEquals("/index1,index2/type1,type2/_endpoint",
                RequestConverters.endpoint(new String[] { "index1", "index2" }, new String[] { "type1", "type2" }, "_endpoint"));
        assertEquals("/index1,index2/_endpoint/suffix1,suffix2",
                RequestConverters.endpoint(new String[] { "index1", "index2" }, "_endpoint", new String[] { "suffix1", "suffix2" }));
    }

    public void testCreateContentType() {
        final XContentType xContentType = randomFrom(XContentType.values());
        assertEquals(xContentType.mediaTypeWithoutParameters(), RequestConverters.createContentType(xContentType).getMimeType());
    }

    public void testEnforceSameContentType() {
        XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);
        IndexRequest indexRequest = new IndexRequest().source(singletonMap("field", "value"), xContentType);
        assertEquals(xContentType, enforceSameContentType(indexRequest, null));
        assertEquals(xContentType, enforceSameContentType(indexRequest, xContentType));

        XContentType bulkContentType = randomBoolean() ? xContentType : null;

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> enforceSameContentType(new IndexRequest().source(singletonMap("field", "value"), XContentType.CBOR),
                        bulkContentType));
        assertEquals("Unsupported content-type found for request with content-type [CBOR], only JSON and SMILE are supported",
                exception.getMessage());

        exception = expectThrows(IllegalArgumentException.class,
                () -> enforceSameContentType(new IndexRequest().source(singletonMap("field", "value"), XContentType.YAML),
                        bulkContentType));
        assertEquals("Unsupported content-type found for request with content-type [YAML], only JSON and SMILE are supported",
                exception.getMessage());

        XContentType requestContentType = xContentType == XContentType.JSON ? XContentType.SMILE : XContentType.JSON;

        exception = expectThrows(IllegalArgumentException.class,
                () -> enforceSameContentType(new IndexRequest().source(singletonMap("field", "value"), requestContentType), xContentType));
        assertEquals("Mismatching content-type found for request with content-type [" + requestContentType + "], "
                + "previous requests have content-type [" + xContentType + "]", exception.getMessage());
    }

    public void testGetLifecyclePolicy() {
        String[] policies = rarely() ? null : randomIndicesNames(0, 10);
        GetLifecyclePolicyRequest req = new GetLifecyclePolicyRequest(policies);
        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(req::setMasterTimeout, TimedRequest.DEFAULT_TIMEOUT, expectedParams);
        setRandomTimeoutTimeValue(req::setTimeout, TimedRequest.DEFAULT_MASTER_TIMEOUT, expectedParams);

        Request request = RequestConverters.getLifecyclePolicy(req);
        assertEquals(request.getMethod(), HttpGet.METHOD_NAME);
        String policiesStr = Strings.arrayToCommaDelimitedString(policies);
        assertEquals(request.getEndpoint(), "/_ilm" + (policiesStr.isEmpty() ? "" : ("/" + policiesStr)));
        assertEquals(request.getParameters(), expectedParams);
    }

    public void testPutLifecyclePolicy() throws Exception {
        String name = randomAlphaOfLengthBetween(2, 20);
        LifecyclePolicy policy = createRandomPolicy(name);
        PutLifecyclePolicyRequest req = new PutLifecyclePolicyRequest(policy);
        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(req::setMasterTimeout, TimedRequest.DEFAULT_MASTER_TIMEOUT, expectedParams);
        setRandomTimeoutTimeValue(req::setTimeout, TimedRequest.DEFAULT_TIMEOUT, expectedParams);

        Request request = RequestConverters.putLifecyclePolicy(req);
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/_ilm/" + name, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
    }

    public void testDeleteLifecycle() {
        String lifecycleName = randomAlphaOfLengthBetween(2,20);
        DeleteLifecyclePolicyRequest req = new DeleteLifecyclePolicyRequest(lifecycleName);
        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(req::setMasterTimeout, TimedRequest.DEFAULT_TIMEOUT, expectedParams);
        setRandomTimeoutTimeValue(req::setTimeout, TimedRequest.DEFAULT_MASTER_TIMEOUT, expectedParams);

        Request request = RequestConverters.deleteLifecyclePolicy(req);
        assertEquals(request.getMethod(), HttpDelete.METHOD_NAME);
        assertEquals(request.getEndpoint(), "/_ilm/" + lifecycleName);
        assertEquals(request.getParameters(), expectedParams);
    }

    public void testSetIndexLifecyclePolicy() throws Exception {
        SetIndexLifecyclePolicyRequest req = new SetIndexLifecyclePolicyRequest();
        String policyName = randomAlphaOfLength(10);
        String[] indices = rarely() ? null : randomIndicesNames(0, 10);
        req.policy(policyName);
        req.indices(indices);
        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(req::setMasterTimeout, SetIndexLifecyclePolicyRequest.DEFAULT_MASTER_TIMEOUT, expectedParams);
        setRandomIndicesOptions(req::indicesOptions, req::indicesOptions, expectedParams);

        Request request = RequestConverters.setIndexLifecyclePolicy(req);
        assertThat(request.getMethod(), equalTo(HttpPut.METHOD_NAME));
        String idxString = Strings.arrayToCommaDelimitedString(indices);
        assertThat(request.getEndpoint(),
            equalTo("/" + (idxString.isEmpty() ? "" : (idxString + "/")) +
                "_ilm/" + policyName));
        assertThat(request.getParameters(), equalTo(expectedParams));
    }

    public void testStartILM() throws Exception {
        StartILMRequest req = new StartILMRequest();
        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(req, expectedParams);
        setRandomTimeout(req::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);

        Request request = RequestConverters.startILM(req);
        assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(request.getEndpoint(), equalTo("/_ilm/start"));
        assertThat(request.getParameters(), equalTo(expectedParams));
    }

    public void testStopILM() throws Exception {
        StopILMRequest req = new StopILMRequest();
        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(req, expectedParams);
        setRandomTimeout(req::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);

        Request request = RequestConverters.stopILM(req);
        assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(request.getEndpoint(), equalTo("/_ilm/stop"));
        assertThat(request.getParameters(), equalTo(expectedParams));
    }

    public void testLifecycleManagementStatus() throws Exception {
        LifecycleManagementStatusRequest req = new LifecycleManagementStatusRequest();
        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(req::setMasterTimeout, TimedRequest.DEFAULT_TIMEOUT, expectedParams);
        setRandomTimeoutTimeValue(req::setTimeout, TimedRequest.DEFAULT_MASTER_TIMEOUT, expectedParams);

        Request request = RequestConverters.lifecycleManagementStatus(req);
        assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
        assertThat(request.getEndpoint(), equalTo("/_ilm/status"));
        assertThat(request.getParameters(), equalTo(expectedParams));
    }

    public void testExplainLifecycle() throws Exception {
        ExplainLifecycleRequest req = new ExplainLifecycleRequest();
        String[] indices = rarely() ? null : randomIndicesNames(0, 10);
        req.indices(indices);
        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(req, expectedParams);
        setRandomIndicesOptions(req::indicesOptions, req::indicesOptions, expectedParams);

        Request request = RequestConverters.explainLifecycle(req);
        assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
        String idxString = Strings.arrayToCommaDelimitedString(indices);
        assertThat(request.getEndpoint(), equalTo("/" + (idxString.isEmpty() ? "" : (idxString + "/")) + "_ilm/explain"));
        assertThat(request.getParameters(), equalTo(expectedParams));
    }

    /**
     * Randomize the {@link FetchSourceContext} request parameters.
     */
    private static void randomizeFetchSourceContextParams(Consumer<FetchSourceContext> consumer, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            if (randomBoolean()) {
                boolean fetchSource = randomBoolean();
                consumer.accept(new FetchSourceContext(fetchSource));
                if (fetchSource == false) {
                    expectedParams.put("_source", "false");
                }
            } else {
                int numIncludes = randomIntBetween(0, 5);
                String[] includes = new String[numIncludes];
                String includesParam = randomFields(includes);
                if (numIncludes > 0) {
                    expectedParams.put("_source_include", includesParam);
                }
                int numExcludes = randomIntBetween(0, 5);
                String[] excludes = new String[numExcludes];
                String excludesParam = randomFields(excludes);
                if (numExcludes > 0) {
                    expectedParams.put("_source_exclude", excludesParam);
                }
                consumer.accept(new FetchSourceContext(true, includes, excludes));
            }
        }
    }

    private static void setRandomSearchParams(SearchRequest searchRequest,
                                              Map<String, String> expectedParams) {
        expectedParams.put(RestSearchAction.TYPED_KEYS_PARAM, "true");
        if (randomBoolean()) {
            searchRequest.routing(randomAlphaOfLengthBetween(3, 10));
            expectedParams.put("routing", searchRequest.routing());
        }
        if (randomBoolean()) {
            searchRequest.preference(randomAlphaOfLengthBetween(3, 10));
            expectedParams.put("preference", searchRequest.preference());
        }
        if (randomBoolean()) {
            searchRequest.searchType(randomFrom(SearchType.CURRENTLY_SUPPORTED));
        }
        expectedParams.put("search_type", searchRequest.searchType().name().toLowerCase(Locale.ROOT));
        if (randomBoolean()) {
            searchRequest.requestCache(randomBoolean());
            expectedParams.put("request_cache", Boolean.toString(searchRequest.requestCache()));
        }
        if (randomBoolean()) {
            searchRequest.allowPartialSearchResults(randomBoolean());
            expectedParams.put("allow_partial_search_results", Boolean.toString(searchRequest.allowPartialSearchResults()));
        }
        if (randomBoolean()) {
            searchRequest.setBatchedReduceSize(randomIntBetween(2, Integer.MAX_VALUE));
        }
        expectedParams.put("batched_reduce_size", Integer.toString(searchRequest.getBatchedReduceSize()));
        if (randomBoolean()) {
            searchRequest.scroll(randomTimeValue());
            expectedParams.put("scroll", searchRequest.scroll().keepAlive().getStringRep());
        }
    }

    static void setRandomIndicesOptions(Consumer<IndicesOptions> setter, Supplier<IndicesOptions> getter,
                                        Map<String, String> expectedParams) {

        if (randomBoolean()) {
            setter.accept(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        }
        expectedParams.put("ignore_unavailable", Boolean.toString(getter.get().ignoreUnavailable()));
        expectedParams.put("allow_no_indices", Boolean.toString(getter.get().allowNoIndices()));
        if (getter.get().expandWildcardsOpen() && getter.get().expandWildcardsClosed()) {
            expectedParams.put("expand_wildcards", "open,closed");
        } else if (getter.get().expandWildcardsOpen()) {
            expectedParams.put("expand_wildcards", "open");
        } else if (getter.get().expandWildcardsClosed()) {
            expectedParams.put("expand_wildcards", "closed");
        } else {
            expectedParams.put("expand_wildcards", "none");
        }
    }

    static void setRandomIncludeDefaults(GetIndexRequest request, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            boolean includeDefaults = randomBoolean();
            request.includeDefaults(includeDefaults);
            if (includeDefaults) {
                expectedParams.put("include_defaults", String.valueOf(includeDefaults));
            }
        }
    }

    static void setRandomHumanReadable(GetIndexRequest request, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            boolean humanReadable = randomBoolean();
            request.humanReadable(humanReadable);
            if (humanReadable) {
                expectedParams.put("human", String.valueOf(humanReadable));
            }
        }
    }

    static void setRandomLocal(Consumer<Boolean> setter, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            boolean local = randomBoolean();
            setter.accept(local);
            if (local) {
                expectedParams.put("local", String.valueOf(local));
            }
        }
    }

    static void setRandomLocal(MasterNodeReadRequest<?> request, Map<String, String> expectedParams) {
        setRandomLocal(request::local, expectedParams);
    }

    static void setRandomTimeout(Consumer<String> setter, TimeValue defaultTimeout, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            String timeout = randomTimeValue();
            setter.accept(timeout);
            expectedParams.put("timeout", timeout);
        } else {
            expectedParams.put("timeout", defaultTimeout.getStringRep());
        }
    }

    private static void setRandomTimeoutTimeValue(Consumer<TimeValue> setter, TimeValue defaultTimeout,
                                                  Map<String, String> expectedParams) {
        if (randomBoolean()) {
            TimeValue timeout = TimeValue.parseTimeValue(randomTimeValue(), "random_timeout");
            setter.accept(timeout);
            expectedParams.put("timeout", timeout.getStringRep());
        } else {
            expectedParams.put("timeout", defaultTimeout.getStringRep());
        }
    }

    static void setRandomMasterTimeout(MasterNodeRequest<?> request, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            String masterTimeout = randomTimeValue();
            request.masterNodeTimeout(masterTimeout);
            expectedParams.put("master_timeout", masterTimeout);
        } else {
            expectedParams.put("master_timeout", MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT.getStringRep());
        }
    }

    private static void setRandomMasterTimeout(Consumer<TimeValue> setter, TimeValue defaultTimeout, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            TimeValue masterTimeout = TimeValue.parseTimeValue(randomTimeValue(), "random_master_timeout");
            setter.accept(masterTimeout);
            expectedParams.put("master_timeout", masterTimeout.getStringRep());
        } else {
            expectedParams.put("master_timeout", defaultTimeout.getStringRep());
        }
    }

    static void setRandomWaitForActiveShards(Consumer<ActiveShardCount> setter, Map<String, String> expectedParams) {
        setRandomWaitForActiveShards(setter, ActiveShardCount.DEFAULT, expectedParams);
    }

    static void setRandomWaitForActiveShards(Consumer<ActiveShardCount> setter, ActiveShardCount defaultActiveShardCount,
                                             Map<String, String> expectedParams) {
        if (randomBoolean()) {
            int waitForActiveShardsInt = randomIntBetween(-1, 5);
            String waitForActiveShardsString;
            if (waitForActiveShardsInt == -1) {
                waitForActiveShardsString = "all";
            } else {
                waitForActiveShardsString = String.valueOf(waitForActiveShardsInt);
            }
            ActiveShardCount activeShardCount = ActiveShardCount.parseString(waitForActiveShardsString);
            setter.accept(activeShardCount);
            if (defaultActiveShardCount.equals(activeShardCount) == false) {
                expectedParams.put("wait_for_active_shards", waitForActiveShardsString);
            }
        }
    }

    private static void setRandomRefreshPolicy(Consumer<WriteRequest.RefreshPolicy> setter, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            WriteRequest.RefreshPolicy refreshPolicy = randomFrom(WriteRequest.RefreshPolicy.values());
            setter.accept(refreshPolicy);
            if (refreshPolicy != WriteRequest.RefreshPolicy.NONE) {
                expectedParams.put("refresh", refreshPolicy.getValue());
            }
        }
    }

    private static void setRandomVersion(DocWriteRequest<?> request, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            long version = randomFrom(Versions.MATCH_ANY, Versions.MATCH_DELETED, Versions.NOT_FOUND, randomNonNegativeLong());
            request.version(version);
            if (version != Versions.MATCH_ANY) {
                expectedParams.put("version", Long.toString(version));
            }
        }
    }

    private static void setRandomVersionType(Consumer<VersionType> setter, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            VersionType versionType = randomFrom(VersionType.values());
            setter.accept(versionType);
            if (versionType != VersionType.INTERNAL) {
                expectedParams.put("version_type", versionType.name().toLowerCase(Locale.ROOT));
            }
        }
    }

    private static String randomFields(String[] fields) {
        StringBuilder excludesParam = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            String exclude = randomAlphaOfLengthBetween(3, 10);
            fields[i] = exclude;
            excludesParam.append(exclude);
            if (i < fields.length - 1) {
                excludesParam.append(",");
            }
        }
        return excludesParam.toString();
    }

    static String[] randomIndicesNames(int minIndicesNum, int maxIndicesNum) {
        int numIndices = randomIntBetween(minIndicesNum, maxIndicesNum);
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = "index-" + randomAlphaOfLengthBetween(2, 5).toLowerCase(Locale.ROOT);
        }
        return indices;
    }
}
