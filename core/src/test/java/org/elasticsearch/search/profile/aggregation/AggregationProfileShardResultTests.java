/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the \"License\"); you may
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

package org.elasticsearch.search.profile.aggregation;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.ProfileResultTests;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class AggregationProfileShardResultTests extends ESTestCase {

    public static AggregationProfileShardResult createTestItem(int depth) {
        int size = randomIntBetween(0, 5);
        List<ProfileResult> aggProfileResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            aggProfileResults.add(ProfileResultTests.createTestItem(1));
        }
        return new AggregationProfileShardResult(aggProfileResults);
    }

    public void testFromXContent() throws IOException {
        AggregationProfileShardResult profileResult = createTestItem(2);
        XContentType xcontentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentFactory.contentBuilder(xcontentType);
        builder.startObject();
        builder = profileResult.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = createParser(builder);
        XContentParserUtils.ensureExpectedToken(parser.nextToken(), XContentParser.Token.START_OBJECT, parser::getTokenLocation);
        XContentParserUtils.ensureFieldName(parser, parser.nextToken(), AggregationProfileShardResult.AGGREGATIONS);
        XContentParserUtils.ensureExpectedToken(parser.nextToken(), XContentParser.Token.START_ARRAY, parser::getTokenLocation);
        AggregationProfileShardResult parsed = AggregationProfileShardResult.fromXContent(parser);
        assertToXContentEquivalent(builder.bytes(), toXContent(parsed, xcontentType), xcontentType);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());
    }

    public void testToXContent() throws IOException {
        List<ProfileResult> profileResults = new ArrayList<>();
        Map<String, Long> timings = new HashMap<>();
        timings.put("timing1", 2000L);
        timings.put("timing2", 4000L);
        ProfileResult profileResult = new ProfileResult("someType", "someDescription", timings, Collections.emptyList());
        profileResults.add(profileResult);
        AggregationProfileShardResult aggProfileResults = new AggregationProfileShardResult(profileResults);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        aggProfileResults.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\"aggregations\":["
                        + "{\"type\":\"someType\","
                            + "\"description\":\"someDescription\","
                            + "\"time\":\"0.006000000000ms\","
                            + "\"breakdown\":{\"timing1\":2000,\"timing2\":4000}"
                        + "}"
                   + "]}", builder.string());
    }

}
