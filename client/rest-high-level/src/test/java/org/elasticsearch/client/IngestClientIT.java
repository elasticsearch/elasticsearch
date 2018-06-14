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

import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.SimulateDocumentBaseResult;
import org.elasticsearch.action.ingest.SimulateDocumentResult;
import org.elasticsearch.action.ingest.SimulateDocumentVerboseResult;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.ingest.WritePipelineResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.PipelineConfiguration;

import java.io.IOException;

public class IngestClientIT extends ESRestHighLevelClientTestCase {

    public void testPutPipeline() throws IOException {
        String id = "some_pipeline_id";
        XContentBuilder pipelineBuilder = buildRandomXContentPipeline();
        PutPipelineRequest request = new PutPipelineRequest(
            id,
            BytesReference.bytes(pipelineBuilder),
            pipelineBuilder.contentType());

        WritePipelineResponse putPipelineResponse =
            execute(request, highLevelClient().ingest()::putPipeline, highLevelClient().ingest()::putPipelineAsync);
        assertTrue(putPipelineResponse.isAcknowledged());
    }

    public void testGetPipeline() throws IOException {
        String id = "some_pipeline_id";
        XContentBuilder pipelineBuilder = buildRandomXContentPipeline();
        {
            PutPipelineRequest request = new PutPipelineRequest(
                id,
                BytesReference.bytes(pipelineBuilder),
                pipelineBuilder.contentType()
            );
            createPipeline(request);
        }

        GetPipelineRequest request = new GetPipelineRequest(id);

        GetPipelineResponse response =
            execute(request, highLevelClient().ingest()::getPipeline, highLevelClient().ingest()::getPipelineAsync);
        assertTrue(response.isFound());
        assertEquals(response.pipelines().get(0).getId(), id);
        PipelineConfiguration expectedConfig =
            new PipelineConfiguration(id, BytesReference.bytes(pipelineBuilder), pipelineBuilder.contentType());
        assertEquals(expectedConfig.getConfigAsMap(), response.pipelines().get(0).getConfigAsMap());
    }

    public void testDeletePipeline() throws IOException {
        String id = "some_pipeline_id";
        {
            createPipeline(id);
        }

        DeletePipelineRequest request = new DeletePipelineRequest(id);

        WritePipelineResponse response =
            execute(request, highLevelClient().ingest()::deletePipeline, highLevelClient().ingest()::deletePipelineAsync);
        assertTrue(response.isAcknowledged());
    }

    public void testSimulatePipeline() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
        boolean isVerbose = randomBoolean();
        builder.startObject();
        {
            builder.field("pipeline");
            buildRandomXContentPipeline(builder);
            builder.startArray("docs");
            {
                builder.startObject()
                    .field("_index", "index")
                    .field("_type", "doc")
                    .field("_id", "doc_" + 1)
                    .startObject("_source").field("foo", "rab_" + 1).field("rank", "1234").endObject()
                    .endObject();
                builder.startObject()
                    .field("_index", "index")
                    .field("_type", "doc")
                    .field("_id", "doc_" + 2)
                    .startObject("_source").field("foo", "rab_" + 1).field("rank", "non-int").endObject()
                    .endObject();
            }
            builder.endArray();
        }
        builder.endObject();

        SimulatePipelineRequest request = new SimulatePipelineRequest(
            BytesReference.bytes(builder),
            builder.contentType()
        );
        request.setVerbose(isVerbose);

        SimulatePipelineResponse simulatePipelineResponse =
            execute(request, highLevelClient().ingest()::simulatePipeline, highLevelClient().ingest()::simulatePipelineAsync);

        SimulateDocumentResult result0 = simulatePipelineResponse.getResults().get(0);
        SimulateDocumentResult result1 = simulatePipelineResponse.getResults().get(1);
        if (isVerbose) {
            assertTrue(result0 instanceof SimulateDocumentVerboseResult);
            SimulateDocumentVerboseResult verboseResult = (SimulateDocumentVerboseResult)result0;
            SimulateDocumentVerboseResult failedVerboseResult = (SimulateDocumentVerboseResult)result1;
            assertTrue(verboseResult.getProcessorResults().size() > 0);
            assertEquals(
                verboseResult.getProcessorResults().get(0).getIngestDocument()
                    .getFieldValue("foo", String.class),
                "bar"
            );
            assertEquals(
                Integer.valueOf(1234),
                verboseResult.getProcessorResults().get(1).getIngestDocument()
                    .getFieldValue("rank", Integer.class)
            );
            assertNotNull(failedVerboseResult.getProcessorResults().get(1).getFailure());
        } else {
            assertTrue(result0 instanceof SimulateDocumentBaseResult);
            SimulateDocumentBaseResult baseResult = (SimulateDocumentBaseResult)result0;
            SimulateDocumentBaseResult failedBaseResult = (SimulateDocumentBaseResult)result0;
            assertNotNull(baseResult.getIngestDocument());
            assertEquals(
                baseResult.getIngestDocument().getFieldValue("foo", String.class),
                "bar"
            );
            assertEquals(
                Integer.valueOf(1234),
                baseResult.getIngestDocument()
                    .getFieldValue("rank", Integer.class)
            );
            assertNotNull(failedBaseResult.getFailure());
        }
    }
}
