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
package org.elasticsearch.protocol.xpack.ml;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetJobsStatsRequestTests extends AbstractXContentTestCase<GetJobsStatsRequest> {

    public void testAllJobsRequest() {
        GetJobsStatsRequest request = GetJobsStatsRequest.allJobsStats();

        assertEquals(request.getJobIds().size(), 1);
        assertEquals(request.getJobIds().get(0), "_all");
    }

    public void testNewWithJobId() {
        Exception exception = expectThrows(NullPointerException.class, () -> new GetJobsStatsRequest("job", null));
        assertEquals(exception.getMessage(), "jobIds must not contain null values");
    }

    @Override
    protected GetJobsStatsRequest createTestInstance() {
        int jobCount = randomIntBetween(0, 10);
        List<String> jobIds = new ArrayList<>(jobCount);

        for (int i = 0; i < jobCount; i++) {
            jobIds.add(randomAlphaOfLength(10));
        }

        GetJobsStatsRequest request = new GetJobsStatsRequest(jobIds);

        if (randomBoolean()) {
            request.setAllowNoJobs(randomBoolean());
        }

        return request;
    }

    @Override
    protected GetJobsStatsRequest doParseInstance(XContentParser parser) throws IOException {
        return GetJobsStatsRequest.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
