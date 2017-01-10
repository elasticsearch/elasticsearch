/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
<<<<<<< eea4db5512fb616608e283d295d583e62d4607c6
 * the Apache License, Version 2.0 (the "License"); you may
=======
 * the Apache License, Version 2.0 (the \"License\"); you may
>>>>>>> Adding parsing to ProfileResult
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

package org.elasticsearch.search.profile;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class ProfileResultTests extends ESTestCase {

    public static ProfileResult createTestItem(int depth) {
        String type = randomAsciiOfLengthBetween(5, 10);
        String description = randomAsciiOfLengthBetween(5, 10);
        int timingsSize = randomIntBetween(0, 5);
        Map<String, Long> timings = new HashMap<>(timingsSize);
        for (int i = 0; i < timingsSize; i++) {
            timings.put(randomAsciiOfLengthBetween(5, 10), randomNonNegativeLong() / timingsSize); // don't overflow Long.MAX_VALUE;
        }
        int childrenSize = depth > 0 ? randomIntBetween(0, 1) : 0;
        List<ProfileResult> children = new ArrayList<>(childrenSize);
        for (int i = 0; i < childrenSize; i++) {
            children.add(createTestItem(depth - 1));
        }
        return new ProfileResult(type, description, timings, children);
    }

    public void testFromXContent() throws IOException {
        ProfileResult profileResult = createTestItem(2);
        XContentType xcontentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentFactory.contentBuilder(xcontentType);
        builder = profileResult.toXContent(builder, ToXContent.EMPTY_PARAMS);

        XContentParser parser = createParser(builder);
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        ProfileResult parsed = ProfileResult.fromXContent(parser);
        assertToXContentEquivalent(builder.bytes(), toXContent(parsed, xcontentType), xcontentType);
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertNull(parser.nextToken());
    }

    public void testToXContent() throws IOException {
        Map<String, Long> timings1 = new HashMap<>();
        timings1.put("key1", 100L);
        Map<String, Long> timings2 = new HashMap<>();
        timings2.put("key1", 123356L);
        List<ProfileResult> children = new ArrayList<>();
        children.add(new ProfileResult("child1", "desc1", timings1, Collections.emptyList()));
        children.add(new ProfileResult("child2", "desc2", timings2, Collections.emptyList()));
        Map<String, Long> timings3 = new HashMap<>();
        timings3.put("key1", 123456L);
        timings3.put("key2", 100000L);
        ProfileResult result = new ProfileResult("someType", "some description", timings3, children);
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"someType\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time_in_nanos\" : 223456,\n" +
                "  \"breakdown\" : {\n" +
                "    \"key1\" : 123456,\n" +
                "    \"key2\" : 100000\n" +
                "  },\n" +
                "  \"children\" : [\n" +
                "    {\n" +
                "      \"type\" : \"child1\",\n" +
                "      \"description\" : \"desc1\",\n" +
                "      \"time_in_nanos\" : 100,\n" +
                "      \"breakdown\" : {\n" +
                "        \"key1\" : 100\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\" : \"child2\",\n" +
                "      \"description\" : \"desc2\",\n" +
                "      \"time_in_nanos\" : 123356,\n" +
                "      \"breakdown\" : {\n" +
                "        \"key1\" : 123356\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
          "}", builder.string());

        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"someType\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time\" : \"123.4micros\",\n" +
                "  \"time_in_nanos\" : 123456,\n" +
                "  \"breakdown\" : {\n" +
                "    \"key1\" : 12345,\n" +
                "    \"key2\" : 6789\n" +
                "  },\n" +
                "  \"children\" : [\n" +
                "    {\n" +
                "      \"type\" : \"child1\",\n" +
                "      \"description\" : \"desc1\",\n" +
                "      \"time\" : \"100nanos\",\n" +
                "      \"time_in_nanos\" : 100,\n" +
                "      \"breakdown\" : { }\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\" : \"child2\",\n" +
                "      \"description\" : \"desc2\",\n" +
                "      \"time\" : \"123.3micros\",\n" +
                "      \"time_in_nanos\" : 123356,\n" +
                "      \"breakdown\" : { }\n" +
                "    }\n" +
                "  ]\n" +
          "}", builder.string());

        result = new ProfileResult("profileName", "some description", Collections.emptyMap(), Collections.emptyList());
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"profileName\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time\" : \"12.3ms\",\n" +
                "  \"time_in_nanos\" : 12345678,\n" +
                "  \"breakdown\" : { }\n" +
              "}", builder.string());

        result = new ProfileResult("profileName", "some description", Collections.emptyMap(), Collections.emptyList());
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"profileName\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time\" : \"1.2s\",\n" +
                "  \"time_in_nanos\" : 1234567890,\n" +
                "  \"breakdown\" : { }\n" +
              "}", builder.string());
    }
}
