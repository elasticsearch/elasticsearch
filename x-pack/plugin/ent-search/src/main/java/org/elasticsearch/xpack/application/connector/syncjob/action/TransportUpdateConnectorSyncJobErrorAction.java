/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobIndexService;

public class TransportUpdateConnectorSyncJobErrorAction extends HandledTransportAction<
    UpdateConnectorSyncJobErrorAction.Request,
    AcknowledgedResponse> {

    protected final ConnectorSyncJobIndexService connectorSyncJobIndexService;

    @Inject
    public TransportUpdateConnectorSyncJobErrorAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(
            UpdateConnectorSyncJobErrorAction.NAME,
            transportService,
            actionFilters,
            UpdateConnectorSyncJobErrorAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.connectorSyncJobIndexService = new ConnectorSyncJobIndexService(client);
    }

    @Override
    protected void doExecute(Task task, UpdateConnectorSyncJobErrorAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        connectorSyncJobIndexService.updateConnectorSyncJobError(
            request.getConnectorSyncJobId(),
            request.getError(),
            listener.map(r -> AcknowledgedResponse.TRUE)
        );
    }
}
