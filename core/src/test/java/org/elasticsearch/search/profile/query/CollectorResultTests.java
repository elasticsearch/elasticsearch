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

package org.elasticsearch.search.profile.query;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class CollectorResultTests extends ESTestCase {

    public static CollectorResult createTestItem(int depth) {
        String name = randomAsciiOfLengthBetween(5, 10);
        String reason = randomAsciiOfLengthBetween(5, 10);
        long time = randomNonNegativeLong();
        int size = randomIntBetween(0, 5);
        List<CollectorResult> children = new ArrayList<>(size);
        if (depth > 0) {
            for (int i = 0; i < size; i++) {
                children.add(createTestItem(depth - 1));
            }
        }
        return new CollectorResult(name, reason, time, children);
    }

    public void testFromXContent() throws IOException {
        CollectorResult collectorResult = createTestItem(1);
        XContentType xcontentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentFactory.contentBuilder(xcontentType);
        builder = collectorResult.toXContent(builder, ToXContent.EMPTY_PARAMS);

        XContentParser parser = createParser(builder);
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        CollectorResult parsed = CollectorResult.fromXContent(parser);
        assertToXContentEquivalent(builder.bytes(), toXContent(parsed, xcontentType), xcontentType);
        assertNull(parser.nextToken());
    }

    public void testToXContent() throws IOException {
        List<CollectorResult> children = new ArrayList<>();
        children.add(new CollectorResult("child1", "reason1", 100L, Collections.emptyList()));
        children.add(new CollectorResult("child2", "reason1", 123356L, Collections.emptyList()));
        CollectorResult result = new CollectorResult("collectorName", "some reason", 123456L, children);
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
            "  \"name\" : \"collectorName\",\n" +
            "  \"reason\" : \"some reason\",\n" +
            "  \"time_in_nanos\" : 123456,\n" +
            "  \"children\" : [\n" +
            "    {\n" +
            "      \"name\" : \"child1\",\n" +
            "      \"reason\" : \"reason1\",\n" +
            "      \"time_in_nanos\" : 100\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\" : \"child2\",\n" +
            "      \"reason\" : \"reason1\",\n" +
            "      \"time_in_nanos\" : 123356\n" +
            "    }\n" +
            "  ]\n" +
          "}", builder.string());

        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
            "  \"name\" : \"collectorName\",\n" +
            "  \"reason\" : \"some reason\",\n" +
            "  \"time\" : \"123.4micros\",\n" +
            "  \"time_in_nanos\" : 123456,\n" +
            "  \"children\" : [\n" +
            "    {\n" +
            "      \"name\" : \"child1\",\n" +
            "      \"reason\" : \"reason1\",\n" +
            "      \"time\" : \"100nanos\",\n" +
            "      \"time_in_nanos\" : 100\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\" : \"child2\",\n" +
            "      \"reason\" : \"reason1\",\n" +
            "      \"time\" : \"123.3micros\",\n" +
            "      \"time_in_nanos\" : 123356\n" +
            "    }\n" +
            "  ]\n" +
          "}", builder.string());

        result = new CollectorResult("collectorName", "some reason", 12345678L, Collections.emptyList());
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"name\" : \"collectorName\",\n" +
                "  \"reason\" : \"some reason\",\n" +
                "  \"time\" : \"12.3ms\",\n" +
                "  \"time_in_nanos\" : 12345678\n" +
              "}", builder.string());

        result = new CollectorResult("collectorName", "some reason", 1234567890L, Collections.emptyList());
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"name\" : \"collectorName\",\n" +
                "  \"reason\" : \"some reason\",\n" +
                "  \"time\" : \"1.2s\",\n" +
                "  \"time_in_nanos\" : 1234567890\n" +
              "}", builder.string());
    }
}
