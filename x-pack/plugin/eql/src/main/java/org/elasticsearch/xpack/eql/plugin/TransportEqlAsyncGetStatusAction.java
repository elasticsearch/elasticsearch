/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;
import org.elasticsearch.xpack.ql.plugin.AbstractTransportQlAsyncGetStatusAction;

public class TransportEqlAsyncGetStatusAction extends AbstractTransportQlAsyncGetStatusAction<EqlSearchResponse, EqlSearchTask> {
    @Inject
    public TransportEqlAsyncGetStatusAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NamedWriteableRegistry registry,
        Client client,
        ThreadPool threadPool,
        BigArrays bigArrays
    ) {
        super(
            EqlAsyncGetStatusAction.NAME,
            transportService,
            actionFilters,
            clusterService,
            registry,
            client,
            threadPool,
            bigArrays,
            EqlSearchTask.class
        );
    }

    @Override
    protected Writeable.Reader<EqlSearchResponse> responseReader() {
        return EqlSearchResponse::new;
    }
}
