/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.ml.job.config.Job;
import org.elasticsearch.protocol.xpack.ml.messages.Messages;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class ValidateJobConfigAction extends Action<ValidateJobConfigAction.Response> {

    public static final ValidateJobConfigAction INSTANCE = new ValidateJobConfigAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/validate";

    protected ValidateJobConfigAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        protected RequestBuilder(ElasticsearchClient client, ValidateJobConfigAction action) {
            super(client, action, new Request());
        }

    }

    public static class Request extends ActionRequest {

        private Job job;

        public static Request parseRequest(XContentParser parser) {
            Job.Builder job = Job.CONFIG_PARSER.apply(parser, null);
            // When jobs are PUT their ID must be supplied in the URL - assume this will
            // be valid unless an invalid job ID is specified in the JSON to be validated
            job.setId(job.getId() != null ? job.getId() : "ok");

            // Some fields cannot be set at create time
            List<String> invalidJobCreationSettings = job.invalidCreateTimeSettings();
            if (invalidJobCreationSettings.isEmpty() == false) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_INVALID_CREATE_SETTINGS,
                        String.join(",", invalidJobCreationSettings)));
            }

            return new Request(job.build(new Date()));
        }

        public Request() {
            this.job = null;
        }

        public Request(Job job) {
            this.job = job;
        }

        public Job getJob() {
            return job;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            job.writeTo(out);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            job = new Job(in);
        }

        @Override
        public int hashCode() {
            return Objects.hash(job);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(job, other.job);
        }

    }

    public static class Response extends AcknowledgedResponse {

        public Response() {
            super();
        }

        public Response(boolean acknowledged) {
            super(acknowledged);
        }
    }

}
