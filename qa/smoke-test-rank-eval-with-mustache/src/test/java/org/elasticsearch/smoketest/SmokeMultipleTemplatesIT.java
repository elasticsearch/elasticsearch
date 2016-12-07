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

package org.elasticsearch.smoketest;

import org.elasticsearch.index.rankeval.Precision;
import org.elasticsearch.index.rankeval.RankEvalAction;
import org.elasticsearch.index.rankeval.RankEvalPlugin;
import org.elasticsearch.index.rankeval.RankEvalRequest;
import org.elasticsearch.index.rankeval.RankEvalRequestBuilder;
import org.elasticsearch.index.rankeval.RankEvalResponse;
import org.elasticsearch.index.rankeval.RankEvalSpec;
import org.elasticsearch.index.rankeval.RatedDocument;
import org.elasticsearch.index.rankeval.RatedRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SmokeMultipleTemplatesIT  extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(RankEvalPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(RankEvalPlugin.class);
    }

    @Before
    public void setup() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "testtype").setId("1")
                .setSource("text", "berlin", "title", "Berlin, Germany").get();
        client().prepareIndex("test", "testtype").setId("2")
                .setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("3")
                .setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("4")
                .setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("5")
                .setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("6")
                .setSource("text", "amsterdam").get();
        refresh();
    }

    public void testPrecisionAtRequest() throws IOException {
        List<String> indices = Arrays.asList(new String[] { "test" });
        List<String> types = Arrays.asList(new String[] { "testtype" });

        List<RatedRequest> specifications = new ArrayList<>();
        RatedRequest amsterdamRequest = new RatedRequest("amsterdam_query", null, indices, types, createRelevant("2", "3", "4", "5"));
        Map<String, Object> ams_params = new HashMap<>();
        ams_params.put("querystring", "amsterdam");
        amsterdamRequest.setParams(ams_params);
        specifications.add(amsterdamRequest);

        RatedRequest berlinRequest = new RatedRequest("berlin_query", null, indices, types, createRelevant("1"));
        Map<String, Object> berlin_params = new HashMap<>();
        berlin_params.put("querystring", "berlin");
        berlinRequest.setParams(berlin_params);
        specifications.add(berlinRequest);

        Precision metric = new Precision();

        Script template = 
                new Script(
                        ScriptType.INLINE,
                        "mustache", "{\"query\": {\"match\": {\"text\": \"{{querystring}}\"}}}",
                        new HashMap<>());
        RankEvalSpec task = new RankEvalSpec(specifications, metric, template);
        RankEvalRequestBuilder builder = new RankEvalRequestBuilder(client(), RankEvalAction.INSTANCE, new RankEvalRequest());
        builder.setRankEvalSpec(task);

        RankEvalResponse response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        assertEquals(0.9, response.getQualityLevel(), Double.MIN_VALUE);
    }

    private static List<RatedDocument> createRelevant(String... docs) {
        List<RatedDocument> relevant = new ArrayList<>();
        for (String doc : docs) {
            relevant.add(new RatedDocument("test", "testtype", doc, Rating.RELEVANT.ordinal()));
        }
        return relevant;
    }

    public enum Rating {
        IRRELEVANT, RELEVANT;
    }

 }
