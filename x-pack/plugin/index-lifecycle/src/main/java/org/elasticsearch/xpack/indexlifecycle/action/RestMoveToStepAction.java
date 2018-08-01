/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 *
 */
package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.indexlifecycle.action.MoveToStepAction;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycle;

import java.io.IOException;

public class RestMoveToStepAction extends BaseRestHandler {

    public RestMoveToStepAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, IndexLifecycle.BASE_PATH + "/move/{name}", this);
    }

    @Override
    public String getName() {
        return "index_lifecycle_move_to_step_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String index = restRequest.param("name");
        XContentParser parser = restRequest.contentParser();
        MoveToStepAction.Request request = MoveToStepAction.Request.parseRequest(index, parser);
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
        return channel -> client.execute(MoveToStepAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
