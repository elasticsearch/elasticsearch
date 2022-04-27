/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorServiceBase;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.ServerHealthComponents.DATA;

/**
 * This indicator reports health for index lifecycle management component.
 *
 * Indicator will report YELLOW status when ILM is not running and there are configured policies.
 * Constant indexing could eventually use entire disk space on hot topology in such cases.
 *
 * ILM must be running to fix warning reported by this indicator.
 */
public class IlmHealthIndicatorService extends HealthIndicatorServiceBase {

    public static final String NAME = "ilm";

    public IlmHealthIndicatorService(ClusterService clusterService) {
        super(clusterService);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String component() {
        return DATA;
    }

    @Override
    public HealthIndicatorResult doCalculate(ClusterState clusterState, boolean includeDetails) {
        var ilmMetadata = clusterState.metadata().custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);
        if (ilmMetadata.getPolicyMetadatas().isEmpty()) {
            return createIndicator(GREEN, "No policies configured", createDetails(includeDetails, ilmMetadata), Collections.emptyList());
        } else if (ilmMetadata.getOperationMode() != OperationMode.RUNNING) {
            return createIndicator(YELLOW, "ILM is not running", createDetails(includeDetails, ilmMetadata), Collections.emptyList());
        } else {
            return createIndicator(GREEN, "ILM is running", createDetails(includeDetails, ilmMetadata), Collections.emptyList());
        }
    }

    private static HealthIndicatorDetails createDetails(boolean includeDetails, IndexLifecycleMetadata metadata) {
        if (includeDetails) {
            return new SimpleHealthIndicatorDetails(
                Map.of("ilm_status", metadata.getOperationMode(), "policies", metadata.getPolicies().size())
            );
        } else {
            return HealthIndicatorDetails.EMPTY;
        }
    }
}
