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
package org.elasticsearch.action.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.ingest.core.IngestDocument;

import java.io.IOException;
import java.util.Collections;

/**
 * Holds the end result of what a pipeline did to sample document provided via the simulate api.
 */
public final class SimulateDocumentBaseResult implements SimulateDocumentResult<SimulateDocumentBaseResult> {

    private static final SimulateDocumentBaseResult PROTOTYPE = new SimulateDocumentBaseResult(new WriteableIngestDocument(new IngestDocument(Collections.emptyMap(), Collections.emptyMap())));

    private WriteableIngestDocument ingestDocument;
    private Exception failure;

    public SimulateDocumentBaseResult(IngestDocument ingestDocument) {
        this.ingestDocument = new WriteableIngestDocument(ingestDocument);
    }

    private SimulateDocumentBaseResult(WriteableIngestDocument ingestDocument) {
        this.ingestDocument = ingestDocument;
    }

    public SimulateDocumentBaseResult(Exception failure) {
        this.failure = failure;
    }

    public IngestDocument getIngestDocument() {
        if (ingestDocument == null) {
            return null;
        }
        return ingestDocument.getIngestDocument();
    }

    public Exception getFailure() {
        return failure;
    }

    public static SimulateDocumentBaseResult readSimulateDocumentSimpleResult(StreamInput in) throws IOException {
        return PROTOTYPE.readFrom(in);
    }

    @Override
    public SimulateDocumentBaseResult readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            Exception exception = in.readThrowable();
            return new SimulateDocumentBaseResult(exception);
        }
        return new SimulateDocumentBaseResult(new WriteableIngestDocument(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (failure == null) {
            out.writeBoolean(false);
            ingestDocument.writeTo(out);
        } else {
            out.writeBoolean(true);
            out.writeThrowable(failure);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (failure == null) {
            ingestDocument.toXContent(builder, params);
        } else {
            ElasticsearchException.renderThrowable(builder, params, failure);
        }
        builder.endObject();
        return builder;
    }
}
