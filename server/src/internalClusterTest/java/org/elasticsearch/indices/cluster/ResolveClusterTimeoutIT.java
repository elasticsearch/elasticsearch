/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.admin.indices.resolve.ResolveClusterActionRequest;
import org.elasticsearch.action.admin.indices.resolve.TransportResolveClusterAction;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ResolveClusterTimeoutIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER_1 = "cluster-a";
    private static final String REMOTE_CLUSTER_2 = "cluster-b";
    private static final String TIME_UNITS = "s";

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
    }

    public void testTimeoutParameterCannotExceedConnectTimeoutSetting() {
        var maxAllowedTimeout = TransportSettings.CONNECT_TIMEOUT.get(cluster(REMOTE_CLUSTER_1).getDefaultSettings()).getSeconds();
        var resolveClusterActionRequest = new ResolveClusterActionRequest(new String[] { "*", "*:*" });

        // Exceed the permissible timeout by 1s.
        resolveClusterActionRequest.setTimeout(maxAllowedTimeout + 1 + "s");
        var ex = expectThrows(
            ExecutionException.class,
            () -> client().execute(TransportResolveClusterAction.TYPE, resolveClusterActionRequest).get()
        );

        // This should trigger an error.
        assertThat(
            ex.getMessage(),
            containsString("Timeout exceeds the value of transport.connect_timeout: " + maxAllowedTimeout + " seconds")
        );
    }

    public void testTimeoutParameter() throws Exception {
        var maxSleepInSeconds = 10L;
        for (var nodes : cluster(REMOTE_CLUSTER_1).getNodeNames()) {
            ((MockTransportService) cluster(REMOTE_CLUSTER_1).getInstance(TransportService.class, nodes)).addRequestHandlingBehavior(
                TransportResolveClusterAction.REMOTE_TYPE.name(),
                (requestHandler, transportRequest, transportChannel, transportTask) -> {
                    Thread.sleep(maxSleepInSeconds * 1000);
                    transportChannel.sendResponse(new IllegalArgumentException());
                }
            );
        }

        for (var nodes : cluster(REMOTE_CLUSTER_2).getNodeNames()) {
            ((MockTransportService) cluster(REMOTE_CLUSTER_2).getInstance(TransportService.class, nodes)).addRequestHandlingBehavior(
                TransportResolveClusterAction.REMOTE_TYPE.name(),
                (requestHandler, transportRequest, transportChannel, transportTask) -> {
                    Thread.sleep(maxSleepInSeconds * 1000);
                    transportChannel.sendResponse(new IllegalArgumentException());
                }
            );
        }

        var resolveClusterActionRequest = new ResolveClusterActionRequest(new String[] { "*", "*:*" });
        var randomlyChosenTimeout = randomLongBetween(3, maxSleepInSeconds - 1);
        resolveClusterActionRequest.setTimeout(randomlyChosenTimeout + TIME_UNITS);

        var start = Instant.now();
        var futureAction = client().execute(TransportResolveClusterAction.TYPE, resolveClusterActionRequest);
        var resolveClusterActionResponse = futureAction.get(maxSleepInSeconds, TimeUnit.SECONDS);
        var end = Instant.now();

        // Ensure that the request took exactly the randomlyChosenTimeout seconds.
        assertThat(Duration.between(start, end).getSeconds(), equalTo(randomlyChosenTimeout));

        var clusterInfo = resolveClusterActionResponse.getResolveClusterInfo();

        // Ensure that the request timed out and that the remotes are marked as not connected.
        assertThat(clusterInfo.get(LOCAL_CLUSTER).isConnected(), equalTo(true));
        assertThat(clusterInfo.get(REMOTE_CLUSTER_1).isConnected(), equalTo(false));
        assertThat(clusterInfo.get(REMOTE_CLUSTER_2).isConnected(), equalTo(false));
    }
}
