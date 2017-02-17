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

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponseTests;
import org.elasticsearch.action.index.IndexResponseTests;
import org.elasticsearch.action.update.UpdateResponseTests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.ElasticsearchExceptionTests.randomExceptions;
import static org.elasticsearch.action.bulk.BulkItemResponseTests.assertBulkItemResponse;
import static org.elasticsearch.action.bulk.BulkResponse.NO_INGEST_TOOK;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class BulkResponseTests extends ESTestCase {

    public void testToAndFromXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();

        long took = randomNonNegativeLong();
        long ingestTook = randomFrom(randomNonNegativeLong(), NO_INGEST_TOOK);
        int nbBulkItems = randomIntBetween(1, 10);

        BulkItemResponse[] bulkItems = new BulkItemResponse[nbBulkItems];
        BulkItemResponse[] expectedBulkItems = new BulkItemResponse[nbBulkItems];

        for (int i = 0; i < nbBulkItems; i++) {
            DocWriteRequest.OpType opType = randomFrom(DocWriteRequest.OpType.values());

            if (frequently()) {
                Tuple<? extends DocWriteResponse, ? extends DocWriteResponse> randomDocWriteResponses = null;
                if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                    randomDocWriteResponses = IndexResponseTests.randomIndexResponse();
                } else if (opType == DocWriteRequest.OpType.DELETE) {
                    randomDocWriteResponses = DeleteResponseTests.randomDeleteResponse();
                } else if (opType == DocWriteRequest.OpType.UPDATE) {
                    randomDocWriteResponses = UpdateResponseTests.randomUpdateResponse(xContentType);
                } else {
                    fail("Test does not support opType [" + opType + "]");
                }

                bulkItems[i] = new BulkItemResponse(i, opType, randomDocWriteResponses.v1());
                expectedBulkItems[i] = new BulkItemResponse(i, opType, randomDocWriteResponses.v2());
            } else {
                String index = randomAsciiOfLength(5);
                String type = randomAsciiOfLength(5);
                String id = randomAsciiOfLength(5);

                Tuple<Throwable, ElasticsearchException> failures = randomExceptions();
                bulkItems[i] = new BulkItemResponse(i, opType, new BulkItemResponse.Failure(index, type, id, (Exception) failures.v1()));
                expectedBulkItems[i] = new BulkItemResponse(i, opType, new BulkItemResponse.Failure(index, type, id, failures.v2()));
            }
        }

        BulkResponse bulkResponse = new BulkResponse(bulkItems, took, ingestTook);
        BytesReference originalBytes = toXContent(bulkResponse, xContentType, humanReadable);

        if (randomBoolean()) {
            try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                originalBytes = shuffleXContent(parser, randomBoolean()).bytes();
            }
        }

        BulkResponse parsedBulkResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parsedBulkResponse = BulkResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        assertEquals(took, parsedBulkResponse.getTookInMillis());
        assertEquals(ingestTook, parsedBulkResponse.getIngestTookInMillis());
        assertEquals(expectedBulkItems.length, parsedBulkResponse.getItems().length);

        for (int i = 0; i < expectedBulkItems.length; i++) {
            assertBulkItemResponse(expectedBulkItems[i], parsedBulkResponse.getItems()[i]);
        }

        BytesReference finalBytes = toXContent(parsedBulkResponse, xContentType, humanReadable);
        BytesReference expectedFinalBytes = toXContent(parsedBulkResponse, xContentType, humanReadable);
        assertToXContentEquivalent(expectedFinalBytes, finalBytes, xContentType);
    }
}
