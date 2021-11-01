/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;

public class InitializePolicyContextStepTests extends AbstractStepTestCase<InitializePolicyContextStep> {

    @Override
    public InitializePolicyContextStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();

        return new InitializePolicyContextStep(stepKey, nextStepKey);
    }

    @Override
    public InitializePolicyContextStep mutateInstance(InitializePolicyContextStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0:
                key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }

        return new InitializePolicyContextStep(key, nextKey);
    }

    @Override
    public InitializePolicyContextStep copyInstance(InitializePolicyContextStep instance) {
        return new InitializePolicyContextStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testAddCreationDate() {
        long creationDate = randomNonNegativeLong();
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .creationDate(creationDate)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Metadata metadata = Metadata.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetadata.builder(indexMetadata))
            .build();
        Index index = indexMetadata.getIndex();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        InitializePolicyContextStep step = new InitializePolicyContextStep(null, null);
        ClusterState newState = step.performAction(index, clusterState);
        assertThat(getIndexLifecycleDate(index, newState), equalTo(creationDate));
    }

    public void testDoNothing() {
        long creationDate = randomNonNegativeLong();
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setIndexCreationDate(creationDate);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .creationDate(creationDate)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Metadata metadata = Metadata.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetadata.builder(indexMetadata))
            .build();
        Index index = indexMetadata.getIndex();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        InitializePolicyContextStep step = new InitializePolicyContextStep(null, null);
        ClusterState newState = step.performAction(index, clusterState);
        assertTrue(newState == clusterState);
    }

    private long getIndexLifecycleDate(Index index, ClusterState clusterState) {
        return LifecycleExecutionState.fromIndexMetadata(clusterState.getMetadata().index(index)).getLifecycleDate();
    }
}
