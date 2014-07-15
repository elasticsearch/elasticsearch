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
package org.elasticsearch.action.benchmark;

import org.apache.lucene.util.English;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.benchmark.competition.*;
import org.elasticsearch.action.benchmark.exception.*;
import org.elasticsearch.action.benchmark.pause.*;
import org.elasticsearch.action.benchmark.resume.*;
import org.elasticsearch.action.benchmark.start.*;
import org.elasticsearch.action.benchmark.status.*;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.BenchmarkMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.action.benchmark.MockBenchmarkExecutorService.MockBenchmarkExecutor;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

import static org.elasticsearch.action.benchmark.BenchmarkTestUtil.*;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for benchmark API
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class BenchmarkIntegrationTest extends ElasticsearchIntegrationTest {

    private int                            numExecutorNodes = 0;
    private Map<String, BenchmarkSettings> competitionSettingsMap;
    private String[]                       indices = Strings.EMPTY_ARRAY;

    protected synchronized Settings nodeSettings(int ordinal) {
        return ImmutableSettings.builder().put("node.bench",
                ordinal == 0 || randomBoolean()).
                    put(BenchmarkModule.BENCHMARK_COORDINATOR_SERVICE_KEY, MockBenchmarkCoordinatorService.class).
                    put(BenchmarkModule.BENCHMARK_EXECUTOR_SERVICE_KEY, MockBenchmarkExecutorService.class).
                    build();
    }

    @Before
    public void pre() throws Exception {

        mockCoordinatorService().clearMockState();
        numExecutorNodes       = cluster().numBenchNodes();
        competitionSettingsMap = new HashMap<>();
        indices                = randomData();

        final Iterable<BenchmarkExecutorService> services = mockExecutorServices();
        for (BenchmarkExecutorService service : services) {
            ((MockBenchmarkExecutorService) service).clearMockState();
        }
    }

    @After
    public void post() throws Exception {
        final BenchmarkStatusResponses responses = client().prepareBenchmarkStatus().execute().actionGet();
        assertThat("Some benchmarks are still running", responses.responses(), is(empty()));
    }

    @Test
    public void testStartBenchmark() throws Exception {

        // Submit benchmark and wait for completion
        final BenchmarkStartRequest request = BenchmarkTestUtil.randomRequest(client(), indices, numExecutorNodes, competitionSettingsMap);
        logger.info("--> Submitting benchmark - competitors [{}] iterations [{}] executors [{}]",
                request.competitors().size(), request.settings().iterations(), numExecutorNodes);
        final BenchmarkStartResponse response = client().startBenchmark(request).actionGet();

        // Validate results
        assertNotNull(response);
        assertThat(response.benchmarkId(), equalTo(BENCHMARK_NAME));
        assertThat(response.state(), equalTo(BenchmarkStartResponse.State.COMPLETED));
        assertFalse(response.hasErrors());
        assertThat(response.competitionResults().size(), equalTo(request.competitors().size()));

        for (CompetitionResult result : response.competitionResults().values()) {
            assertThat(result.nodeResults().size(), equalTo(numExecutorNodes));
            validateCompetitionResult(result, competitionSettingsMap.get(result.competitionName()), true);
        }

        // Confirm that cluster metadata went through proper state transitions
        mockCoordinatorService().validateNormalLifecycle(BENCHMARK_NAME, numExecutorNodes);
    }

    @Test
    public void testPauseBenchmark() throws Exception {

        // Submit benchmark and wait for completion
        final BenchmarkStartRequest request = BenchmarkTestUtil.randomRequest(client(), indices, numExecutorNodes,
                competitionSettingsMap, MEDIUM_LOW_INTERVAL_BOUND, MEDIUM_HIGH_INTERVAL_BOUND);
        logger.info("--> Submitting benchmark - competitors [{}] iterations [{}] executors [{}]",
                request.competitors().size(), request.settings().iterations(), numExecutorNodes);

        // Figure out where to suspend execution
        final int competitorToPause      = randomIntBetween(0, request.competitors().size() - 1);
        final int iterationToPauseBefore = randomIntBetween(0, request.competitors().get(competitorToPause).settings().iterations() - 1);

        final Tuple<CyclicBarrier, List<MockBenchmarkExecutor.FlowControl>> tuple =
                                            setUpFlowControl(request, competitorToPause, iterationToPauseBefore);

        // Start benchmark
        final ActionFuture<BenchmarkStartResponse> future = client().startBenchmark(request);

        // Wait for all executors to initialize
        tuple.v1().await();

        // Check status
        final BenchmarkStatusResponses statusResponses1 = client().prepareBenchmarkStatus(BENCHMARK_NAME).execute().actionGet();
        assertThat(statusResponses1.responses().size(), equalTo(1));
        final BenchmarkStartResponse statusResponse1 = statusResponses1.responses().get(0);
        assertThat(statusResponse1.benchmarkId(), equalTo(BENCHMARK_NAME));
        assertThat(statusResponse1.state(), equalTo(BenchmarkStartResponse.State.RUNNING));
        assertFalse(statusResponse1.hasErrors());

        // Pause benchmark
        final BenchmarkPauseResponse pauseResponse = client().preparePauseBenchmark(BENCHMARK_NAME).execute().actionGet();
        final Map<String, BenchmarkMetaData.Entry.NodeState> nodeResponses = pauseResponse.getNodeResponses();
        assertThat(nodeResponses.size(), equalTo(numExecutorNodes));
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> entry : nodeResponses.entrySet()) {
            assertThat(entry.getValue(), equalTo(BenchmarkMetaData.Entry.NodeState.PAUSED));
        }

        // Check status
        final BenchmarkStatusResponses statusResponses2 = client().prepareBenchmarkStatus(BENCHMARK_NAME).execute().actionGet();
        assertThat(statusResponses2.responses().size(), equalTo(1));
        final BenchmarkStartResponse statusResponse2 = statusResponses2.responses().get(0);
        assertThat(statusResponse2.benchmarkId(), equalTo(BENCHMARK_NAME));
        assertThat(statusResponse2.state(), equalTo(BenchmarkStartResponse.State.PAUSED));
        assertFalse(statusResponse2.hasErrors());

        // Release flow control and let the benchmark complete
        for (MockBenchmarkExecutor.FlowControl control : tuple.v2()) {
            control.release();
        }

        // Resume benchmark
        final BenchmarkResumeResponse resumeResponse = client().prepareResumeBenchmark(BENCHMARK_NAME).execute().actionGet();
        Map<String, BenchmarkMetaData.Entry.NodeState> nodeResponses1 = resumeResponse.getNodeResponses();
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> entry : nodeResponses1.entrySet()) {
            assertThat(entry.getValue(), equalTo(BenchmarkMetaData.Entry.NodeState.RUNNING));
        }

        // Validate results
        final BenchmarkStartResponse benchmarkStartResponse = future.get();
        assertNotNull(benchmarkStartResponse);
        assertThat(benchmarkStartResponse.benchmarkId(), equalTo(BENCHMARK_NAME));
        assertThat(benchmarkStartResponse.state(), equalTo(BenchmarkStartResponse.State.COMPLETED));
        assertFalse(benchmarkStartResponse.hasErrors());
        assertThat(benchmarkStartResponse.competitionResults().size(), equalTo(request.competitors().size()));

        for (CompetitionResult result : benchmarkStartResponse.competitionResults().values()) {
            assertThat(result.nodeResults().size(), equalTo(numExecutorNodes));
            validateCompetitionResult(result, competitionSettingsMap.get(result.competitionName()), true);
        }

        // Confirm that cluster metadata went through proper state transitions
        mockCoordinatorService().validatePausedLifecycle(BENCHMARK_NAME, numExecutorNodes);
    }

    @Test
    @Ignore
    public void testAbortBenchmark() throws Exception {

        // Submit benchmark and wait for completion
        final BenchmarkStartRequest request =
                BenchmarkTestUtil.randomRequest(client(), indices, numExecutorNodes, competitionSettingsMap);
        logger.info("--> Submitting benchmark - competitors [{}] iterations [{}] executors [{}]",
                request.competitors().size(), request.settings().iterations(), numExecutorNodes);

        // Figure out where to suspend execution
        final int competitorToPause      = randomIntBetween(0, request.competitors().size() - 1);
        final int iterationToPauseBefore = randomIntBetween(0, request.competitors().get(competitorToPause).settings().iterations() - 1);

        final Tuple<CyclicBarrier, List<MockBenchmarkExecutor.FlowControl>> tuple =
                setUpFlowControl(request, competitorToPause, iterationToPauseBefore);

        // Start benchmark
        final ActionFuture<BenchmarkStartResponse> future = client().startBenchmark(request);

        // Wait for all executors to initialize
        tuple.v1().await();

        // XXX - Finish


        // Release flow control and let the benchmark complete
        for (MockBenchmarkExecutor.FlowControl control : tuple.v2()) {
            control.release();
        }

        // Confirm that cluster metadata went through proper state transitions
        mockCoordinatorService().validateAbortedLifecycle(BENCHMARK_NAME, numExecutorNodes);
    }

    @Test
    @Ignore
    public void testAbortByPattern() throws Exception {

        // XXX - Finish
    }

    @Test
    @Ignore
    public void testBenchmarkWithErrors() {
        List<SearchRequest> reqList = new ArrayList<>();
        int numQueries = scaledRandomIntBetween(20, 100);
        int numErrors = scaledRandomIntBetween(1, numQueries);
        final boolean containsFatal = randomBoolean();
        if (containsFatal) {
            ScriptScoreFunctionBuilder scriptFunction = scriptFunction("DOES NOT COMPILE - fails on any shard");
            SearchRequest searchRequest = searchRequest().source(
                    searchSource()
                            .query(functionScoreQuery(FilterBuilders.matchAllFilter(), scriptFunction)));
            reqList.add(searchRequest);

        }
        for (int i = 0; reqList.size() < numErrors; i++) {
            ScriptScoreFunctionBuilder scriptFunction = scriptFunction("throw new RuntimeException();");
            SearchRequest searchRequest = searchRequest().source(
                    searchSource()
                            .query(functionScoreQuery(FilterBuilders.matchAllFilter(), scriptFunction)));
            reqList.add(searchRequest);
        }
        logger.info("--> run with [{}] errors ", numErrors);
        for (int i = 0; reqList.size() < numQueries; i++) {

            reqList.add(BenchmarkTestUtil.randomSearch(client(), indices));
        }
        Collections.shuffle(reqList, getRandom());

        final BenchmarkStartRequest request =
                BenchmarkTestUtil.randomRequest(client(),indices, numExecutorNodes, competitionSettingsMap, reqList.toArray(new SearchRequest[0]));
        logger.info("--> Submitting benchmark - competitors [{}] iterations [{}]", request.competitors().size(),
                request.settings().iterations());
        final BenchmarkStartResponse response = client().startBenchmark(request).actionGet();

        assertThat(response, notNullValue());
        if (response.hasErrors() || containsFatal) {
            assertThat(response.state(), equalTo(BenchmarkStartResponse.State.FAILED));
        } else {
            assertThat(response.state(), equalTo(BenchmarkStartResponse.State.COMPLETED));
            for (CompetitionResult result : response.competitionResults().values()) {
                assertThat(result.nodeResults().size(), equalTo(numExecutorNodes));
                validateCompetitionResult(result, competitionSettingsMap.get(result.competitionName()), true);
            }
        }
        assertThat(response.benchmarkId(), equalTo(BENCHMARK_NAME));
    }

    @Test(expected = BenchmarkMissingException.class)
    @Ignore
    public void testAbortNoSuchBenchmark() throws Exception {
        client().prepareAbortBench(BENCHMARK_NAME).execute().actionGet();
    }

    private void validateCompetitionResult(CompetitionResult result, BenchmarkSettings requestedSettings, boolean strict) {
        // Validate settings
        assertTrue(result.competitionName().startsWith(COMPETITOR_PREFIX));
        assertThat(result.concurrency(), equalTo(requestedSettings.concurrency()));
        assertThat(result.multiplier(), equalTo(requestedSettings.multiplier()));

        // Validate node-level responses
        for (CompetitionNodeResult nodeResult : result.nodeResults()) {

            assertThat(nodeResult.nodeName(), notNullValue());

            assertThat(nodeResult.totalIterations(), equalTo(requestedSettings.iterations()));
            if (strict) {
                assertThat(nodeResult.completedIterations(), equalTo(requestedSettings.iterations()));
                final int expectedQueryCount = requestedSettings.multiplier() *
                        nodeResult.totalIterations() * requestedSettings.searchRequests().size();
                assertThat(nodeResult.totalExecutedQueries(), equalTo(expectedQueryCount));
                assertThat(nodeResult.iterations().size(), equalTo(requestedSettings.iterations()));
            }

            assertThat(nodeResult.warmUpTime(), greaterThanOrEqualTo(0L));

            for (CompetitionIteration iteration : nodeResult.iterations()) {
                // Basic sanity checks
                iteration.computeStatistics();
                assertThat(iteration.totalTime(), greaterThanOrEqualTo(0L));
                assertThat(iteration.min(), greaterThanOrEqualTo(0L));
                assertThat(iteration.max(), greaterThanOrEqualTo(iteration.min()));
                assertThat(iteration.mean(), greaterThanOrEqualTo((double) iteration.min()));
                assertThat(iteration.mean(), lessThanOrEqualTo((double) iteration.max()));
                assertThat(iteration.queriesPerSecond(), greaterThanOrEqualTo(0.0));
                assertThat(iteration.millisPerHit(), greaterThanOrEqualTo(0.0));
                validatePercentiles(iteration.percentileValues());
            }
        }

        // Validate summary statistics
        final CompetitionSummary summary = result.competitionSummary();
        summary.computeSummaryStatistics();
        assertThat(summary, notNullValue());
        assertThat(summary.getMin(), greaterThanOrEqualTo(0L));
        assertThat(summary.getMax(), greaterThanOrEqualTo(summary.getMin()));
        assertThat(summary.getMean(), greaterThanOrEqualTo((double) summary.getMin()));
        assertThat(summary.getMean(), lessThanOrEqualTo((double) summary.getMax()));
        assertThat(summary.getTotalTime(), greaterThanOrEqualTo(0L));
        assertThat(summary.getQueriesPerSecond(), greaterThanOrEqualTo(0.0));
        assertThat(summary.getMillisPerHit(), greaterThanOrEqualTo(0.0));
        assertThat(summary.getAvgWarmupTime(), greaterThanOrEqualTo(0.0));
        if (strict) {
            assertThat((int) summary.getTotalIterations(), equalTo(requestedSettings.iterations() * summary.nodeResults().size()));
            assertThat((int) summary.getCompletedIterations(), equalTo(requestedSettings.iterations() * summary.nodeResults().size()));
            assertThat((int) summary.getTotalQueries(), equalTo(requestedSettings.iterations() * requestedSettings.multiplier() *
                    requestedSettings.searchRequests().size() * summary.nodeResults().size()));
            validatePercentiles(summary.getPercentileValues());
        }
    }

    private void validatePercentiles(Map<Double, Double> percentiles) {
        int i = 0;
        double last = Double.NEGATIVE_INFINITY;
        for (Map.Entry<Double, Double> entry : percentiles.entrySet()) {
            assertThat(entry.getKey(), equalTo(BenchmarkSettings.DEFAULT_PERCENTILES[i++]));
            // This is a hedge against rounding errors. Sometimes two adjacent percentile values will
            // be nearly equivalent except for some insignificant decimal places. In such cases we
            // want the two values to compare as equal.
            assertThat(entry.getValue(), greaterThanOrEqualTo(last - 1e-6));
            last = entry.getValue();
        }
    }

    private String[] randomData() throws Exception {

        final int numIndices = scaledRandomIntBetween(1, 5);
        final String[] indices = new String[numIndices];

        for (int i = 0; i < numIndices; i++) {
            indices[i] = INDEX_PREFIX + i;
            final int numDocs = scaledRandomIntBetween(1, 100);
            final IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];

            for (int j = 0; j < numDocs; j++) {
                docs[j] = client().prepareIndex(indices[i], INDEX_TYPE).
                        setSource(BenchmarkTestUtil.TestIndexField.INT_FIELD.toString(), randomInt(),
                                  BenchmarkTestUtil.TestIndexField.FLOAT_FIELD.toString(), randomFloat(),
                                  BenchmarkTestUtil.TestIndexField.BOOLEAN_FIELD.toString(), randomBoolean(),
                                  BenchmarkTestUtil.TestIndexField.STRING_FIELD.toString(), English.intToEnglish(j));
            }

            indexRandom(true, docs);
        }

        flushAndRefresh();
        return indices;
    }

    private Tuple<CyclicBarrier, List<MockBenchmarkExecutor.FlowControl>> setUpFlowControl(
            final BenchmarkStartRequest request,
            final int competitorToPause,
            final int iterationToPauseBefore) throws InterruptedException {

        logger.info("--> Pausing competitor [{} (out of total competitors {})] before iteration [{} (out of total iterations {})]",
                request.competitors().get(competitorToPause).name(), request.competitors().size(), iterationToPauseBefore,
                request.competitors().get(competitorToPause).settings().iterations());

        final List<MockBenchmarkExecutor.FlowControl> controls = new ArrayList<>();
        final CyclicBarrier                           barrier  = new CyclicBarrier(request.numExecutorNodes() + 1);

        for (BenchmarkExecutorService mock : mockExecutorServices()) {

            final MockBenchmarkExecutor executor  = ((MockBenchmarkExecutorService) mock).executor();
            final Semaphore             semaphore = new Semaphore(1);

            final MockBenchmarkExecutor.FlowControl control =
                    new MockBenchmarkExecutor.FlowControl(request.competitors().get(competitorToPause).name(),
                                                          iterationToPauseBefore,
                                                          semaphore);

            controls.add(control);
            semaphore.acquire();

            executor.flow                  = control;
            executor.initializationBarrier = barrier;
        }

        return new Tuple<>(barrier, controls);
    }

    private Iterable<BenchmarkExecutorService> mockExecutorServices() {
        return internalCluster().getInstances(BenchmarkExecutorService.class);
    }

    private MockBenchmarkCoordinatorService mockCoordinatorService() {

        // Don't use mock service class for getInstances(), otherwise we won't get the singleton.
        // Use the base service class instead and cast to the mock service
        final Iterable<BenchmarkCoordinatorService> services = internalCluster().getInstances(BenchmarkCoordinatorService.class);

        for (BenchmarkCoordinatorService service : services) {
            // The instance on the master node will have all the published meta-data changes
            if (((MockBenchmarkCoordinatorService) service).isOnMasterNode()) {
                return (MockBenchmarkCoordinatorService) service;
            }
        }
        fail("Unable to find mock benchmark coordinator service on master node");
        return null;
    }
}
