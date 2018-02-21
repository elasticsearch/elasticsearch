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

package org.elasticsearch.action.admin.indices.delete;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

public class DeleteIndexResponseTests extends AbstractStreamableXContentTestCase<DeleteIndexResponse> {

    public void testToXContent() {
        DeleteIndexResponse response = new DeleteIndexResponse(true);
        String output = Strings.toString(response);
        assertEquals("{\"acknowledged\":true}", output);
    }

    @Override
    protected DeleteIndexResponse doParseInstance(XContentParser parser) {
        return DeleteIndexResponse.fromXContent(parser);
    }

    @Override
    protected DeleteIndexResponse createTestInstance() {
        return new DeleteIndexResponse(randomBoolean());
    }

    @Override
    protected DeleteIndexResponse createBlankInstance() {
        return new DeleteIndexResponse();
    }

    @Override
    protected EqualsHashCodeTestUtils.MutateFunction<DeleteIndexResponse> getMutateFunction() {
        return response -> new DeleteIndexResponse(response.isAcknowledged() == false);
    }
}
