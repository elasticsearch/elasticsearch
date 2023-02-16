/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class GetEngineAction extends ActionType<GetEngineAction.Response> {

    public static final GetEngineAction INSTANCE = new GetEngineAction();
    public static final String NAME = "cluster:admin/engine/get"; // TODO verify this

    private GetEngineAction() {
        super(NAME, GetEngineAction.Response::new);
    }

    public static class Request extends ActionRequest {

        private final String engineId;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.engineId = in.readString();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request(String engineId) {
            this.engineId = engineId;
        }

        public String getEngineId() {
            return engineId;
        }
    }

    // TODO add CreatedAt, UpdatedAt
    public static class Response extends ActionResponse implements ToXContentObject {

        private final String engineId;
        private final String[] indices;
        private final String analyticsCollectionName; // TODO should this be optional?


        public Response(StreamInput in) throws IOException {
            super(in);
            this.engineId = in.readString();
            this.indices = in.readStringArray();
            this.analyticsCollectionName = in.readString(); // TODO: Check this is needed
        }

        public Response(String engineId, String[] indices, String analyticsCollectionName) {
            this.engineId = engineId;
            this.indices = indices;
            this.analyticsCollectionName = analyticsCollectionName;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(engineId);
            out.writeStringArray(indices);
            out.writeString(analyticsCollectionName);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("engine_id", this.engineId);
            builder.array("indices", this.indices);
            builder.field("analytics_collection_name", this.analyticsCollectionName);
            builder.endObject();
            return builder;
        }

    }
}
