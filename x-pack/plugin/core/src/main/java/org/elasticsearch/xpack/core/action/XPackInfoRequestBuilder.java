/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.license.XPackInfoResponse;

import java.util.EnumSet;

public class XPackInfoRequestBuilder extends ActionRequestBuilder<XPackInfoRequest, XPackInfoResponse, XPackInfoRequestBuilder> {

    public XPackInfoRequestBuilder(ElasticsearchClient client) {
        this(client, XPackInfoAction.INSTANCE);
    }

    public XPackInfoRequestBuilder(ElasticsearchClient client, XPackInfoAction action) {
        super(client, action, new XPackInfoRequest());
    }

    public XPackInfoRequestBuilder setVerbose(boolean verbose) {
        request.setVerbose(verbose);
        return this;
    }


    public XPackInfoRequestBuilder setCategories(EnumSet<XPackInfoRequest.Category> categories) {
        request.setCategories(categories);
        return this;
    }

}
