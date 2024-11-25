/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class UnifiedCompletionAction extends ActionType<InferenceAction.Response> {
    public static final UnifiedCompletionAction INSTANCE = new UnifiedCompletionAction();
    public static final String NAME = "cluster:monitor/xpack/inference/unified";

    public UnifiedCompletionAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest {
        public static Request parseRequest(String inferenceEntityId, TaskType taskType, XContentParser parser) throws IOException {
            var unifiedRequest = UnifiedCompletionRequest.PARSER.apply(parser, null);
            return new Request(inferenceEntityId, taskType, unifiedRequest);
        }

        private final String inferenceEntityId;
        private final TaskType taskType;
        private final UnifiedCompletionRequest unifiedCompletionRequest;

        public Request(String inferenceEntityId, TaskType taskType, UnifiedCompletionRequest unifiedCompletionRequest) {
            this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
            this.taskType = Objects.requireNonNull(taskType);
            this.unifiedCompletionRequest = Objects.requireNonNull(unifiedCompletionRequest);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.inferenceEntityId = in.readString();
            this.taskType = TaskType.fromStream(in);
            this.unifiedCompletionRequest = new UnifiedCompletionRequest(in);
        }

        public TaskType getTaskType() {
            return taskType;
        }

        public String getInferenceEntityId() {
            return inferenceEntityId;
        }

        public UnifiedCompletionRequest getUnifiedCompletionRequest() {
            return unifiedCompletionRequest;
        }

        public boolean isStreaming() {
            return Objects.requireNonNullElse(unifiedCompletionRequest.stream(), false);
        }

        @Override
        public ActionRequestValidationException validate() {
            if (unifiedCompletionRequest == null || unifiedCompletionRequest.messages() == null) {
                var e = new ActionRequestValidationException();
                e.addValidationError("Field [messages] cannot be null");
                return e;
            }

            if (unifiedCompletionRequest.messages().isEmpty()) {
                var e = new ActionRequestValidationException();
                e.addValidationError("Field [messages] cannot be an empty array");
                return e;
            }

            if (taskType != TaskType.COMPLETION) {
                var e = new ActionRequestValidationException();
                e.addValidationError("Field [taskType] must be [completion]");
                return e;
            }

            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(inferenceEntityId);
            taskType.writeTo(out);
            unifiedCompletionRequest.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(inferenceEntityId, request.inferenceEntityId)
                && taskType == request.taskType
                && Objects.equals(unifiedCompletionRequest, request.unifiedCompletionRequest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inferenceEntityId, taskType, unifiedCompletionRequest);
        }
    }

}
