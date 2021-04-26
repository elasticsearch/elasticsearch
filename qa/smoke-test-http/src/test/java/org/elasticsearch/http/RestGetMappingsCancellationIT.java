/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskInfo;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.function.Function;

import static org.hamcrest.core.IsEqual.equalTo;

public class RestGetMappingsCancellationIT extends HttpSmokeTestCase {
    public void testGetMappingsCancellation() throws Exception {
        createIndex("test");
        ensureGreen("test");
        final String actionName = GetMappingsAction.NAME;
        // Add a retryable cluster block that would block the request execution
        transformClusterState(currentState -> {
            ClusterBlock clusterBlock = new ClusterBlock(1000,
                "",
                true,
                false,
                false,
                RestStatus.BAD_REQUEST,
                EnumSet.of(ClusterBlockLevel.METADATA_READ)
            );

            return ClusterState.builder(currentState)
                .blocks(ClusterBlocks.builder().addGlobalBlock(clusterBlock).build())
                .build();
        });

        final Request request = new Request(HttpGet.METHOD_NAME, "/test/_mappings");
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        final Cancellable cancellable = getRestClient().performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                future.onResponse(null);
            }

            @Override
            public void onFailure(Exception exception) {
                future.onFailure(exception);
            }
        });

        assertThat(future.isDone(), equalTo(false));
        awaitTaskWithPrefix(actionName);

        cancellable.cancel();

        // Remove the cluster block
        transformClusterState(currentState -> ClusterState.builder(currentState).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build());

        expectThrows(CancellationException.class, future::actionGet);

        assertAllCancellableTasksAreCancelled(actionName);
        assertBusy(() -> {
            final List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().get().getTasks();
            assertTrue(tasks.toString(), tasks.stream().noneMatch(t -> t.getAction().startsWith(actionName)));
        });
    }

    private void transformClusterState(Function<ClusterState, ClusterState> transformationFn) {
        final TimeValue timeout = TimeValue.timeValueSeconds(10);

        final AckedRequest ackedRequest = new AckedRequest() {
            @Override
            public TimeValue ackTimeout() {
                return timeout;
            }

            @Override
            public TimeValue masterNodeTimeout() {
                return timeout;
            }
        };

        PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();
        internalCluster().getMasterNodeInstance(ClusterService.class).submitStateUpdateTask("get_mappings_cancellation_test",
            new AckedClusterStateUpdateTask(ackedRequest, future) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return transformationFn.apply(currentState);
                }
            });

        future.actionGet();
    }
}
