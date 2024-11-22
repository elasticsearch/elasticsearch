/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.datastreams.CancelReindexDataStreamAction;
import org.elasticsearch.action.datastreams.CancelReindexDataStreamAction.Request;
import org.elasticsearch.action.datastreams.CancelReindexDataStreamAction.Response;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.datastreams.task.ReindexDataStreamTask;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Optional;

public class CancelReindexDataStreamTransportAction extends HandledTransportAction<Request, Response> {
    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public CancelReindexDataStreamTransportAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(CancelReindexDataStreamAction.NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        String persistentTaskId = request.getPersistentTaskId();
        PersistentTasksCustomMetadata persistentTasksCustomMetadata = clusterService.state()
            .getMetadata()
            .custom(PersistentTasksCustomMetadata.TYPE);
        PersistentTasksCustomMetadata.PersistentTask<?> persistentTask = persistentTasksCustomMetadata.getTask(persistentTaskId);
        if (persistentTask == null) {
            listener.onFailure(new ElasticsearchException("No persistent task found with id [{}]", persistentTaskId));
        } else if (persistentTask.isAssigned()) {
            String nodeId = persistentTask.getExecutorNode();
            if (clusterService.localNode().getId().equals(nodeId)) {
                getRunningTaskFromNode(persistentTaskId, listener);
            } else {
                runOnNodeWithTaskIfPossible(task, request, nodeId, listener);
            }
        } else {
            listener.onFailure(new ElasticsearchException("Persistent task with id [{}] is not assigned to a node", persistentTaskId));
        }
    }

    private ReindexDataStreamTask getRunningPersistentTaskFromTaskManager(String persistentTaskId) {
        Optional<Map.Entry<Long, CancellableTask>> optionalTask = taskManager.getCancellableTasks()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().getType().equals("persistent"))
            .filter(
                entry -> entry.getValue() instanceof ReindexDataStreamTask
                    && persistentTaskId.equals((((AllocatedPersistentTask) entry.getValue()).getPersistentTaskId()))
            )
            .findAny();
        return optionalTask.map(entry -> (ReindexDataStreamTask) entry.getValue()).orElse(null);
    }

    void getRunningTaskFromNode(String persistentTaskId, ActionListener<Response> listener) {
        ReindexDataStreamTask runningTask = getRunningPersistentTaskFromTaskManager(persistentTaskId);
        if (runningTask == null) {
            listener.onFailure(
                new ResourceNotFoundException(
                    Strings.format(
                        "Persistent task [{}] is supposed to be running on node [{}], " + "but the task is not found on that node",
                        persistentTaskId,
                        clusterService.localNode().getId()
                    )
                )
            );
        } else {
            TaskInfo info = runningTask.taskInfo(clusterService.localNode().getId(), true);
            runningTask.markAsCompleted();
            listener.onResponse(new Response(new TaskResult(true, info)));
        }
    }

    private void runOnNodeWithTaskIfPossible(Task thisTask, Request request, String nodeId, ActionListener<Response> listener) {
        DiscoveryNode node = clusterService.state().nodes().get(nodeId);
        if (node == null) {
            listener.onFailure(
                new ResourceNotFoundException(
                    Strings.format(
                        "Persistent task [{}] is supposed to be running on node [{}], but that node is not part of the cluster",
                        request.getPersistentTaskId(),
                        nodeId
                    )
                )
            );
        } else {
            Request nodeRequest = request.nodeRequest(clusterService.localNode().getId(), thisTask.getId());
            transportService.sendRequest(
                node,
                CancelReindexDataStreamAction.NAME,
                nodeRequest,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener, Response::new, EsExecutors.DIRECT_EXECUTOR_SERVICE)
            );
        }
    }
}
