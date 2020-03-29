/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TransportPutAutoscalingPolicyActionTests extends AutoscalingTestCase {

    public void testAddPolicy() {
        final ClusterState currentState;
        {
            final ClusterState.Builder builder = ClusterState.builder(new ClusterName(randomAlphaOfLength(8)));
            if (randomBoolean()) {
                builder.metaData(MetaData.builder().putCustom(AutoscalingMetadata.NAME, randomAutoscalingMetadata()));
            }
            currentState = builder.build();
        }
        // put an entirely new policy
        final AutoscalingPolicy policy = randomAutoscalingPolicy();
        final Logger mockLogger = mock(Logger.class);
        final ClusterState state = TransportPutAutoscalingPolicyAction.putAutoscalingPolicy(currentState, policy, mockLogger);

        // ensure the new policy is in the updated cluster state
        final AutoscalingMetadata metadata = state.metaData().custom(AutoscalingMetadata.NAME);
        assertNotNull(metadata);
        assertThat(metadata.policies(), hasKey(policy.name()));
        assertThat(metadata.policies().get(policy.name()).policy(), equalTo(policy));
        verify(mockLogger).info("adding autoscaling policy [{}]", policy.name());
        verifyNoMoreInteractions(mockLogger);

        // ensure that existing policies were preserved
        final AutoscalingMetadata currentMetadata = currentState.metaData().custom(AutoscalingMetadata.NAME);
        if (currentMetadata != null) {
            for (final Map.Entry<String, AutoscalingPolicyMetadata> entry : currentMetadata.policies().entrySet()) {
                assertThat(metadata.policies(), hasKey(entry.getKey()));
                assertThat(metadata.policies().get(entry.getKey()).policy(), equalTo(entry.getValue().policy()));
            }
        }
    }

    public void testUpdatePolicy() {
        final ClusterState currentState;
        {
            final ClusterState.Builder builder = ClusterState.builder(new ClusterName(randomAlphaOfLength(8)));
            builder.metaData(
                MetaData.builder().putCustom(AutoscalingMetadata.NAME, randomAutoscalingMetadataOfPolicyCount(randomIntBetween(1, 8)))
            );
            currentState = builder.build();
        }
        final AutoscalingMetadata currentMetadata = currentState.metaData().custom(AutoscalingMetadata.NAME);
        final String name = randomFrom(currentMetadata.policies().keySet());
        // add to the existing deciders, to ensure the policy has changed
        final AutoscalingPolicy policy = new AutoscalingPolicy(
            name,
            Stream.concat(currentMetadata.policies().get(name).policy().deciders().stream(), randomAutoscalingDeciders().stream())
                .collect(Collectors.toUnmodifiableList())
        );
        final Logger mockLogger = mock(Logger.class);
        final ClusterState state = TransportPutAutoscalingPolicyAction.putAutoscalingPolicy(currentState, policy, mockLogger);

        // ensure the updated policy is in the updated cluster state
        final AutoscalingMetadata metadata = state.metaData().custom(AutoscalingMetadata.NAME);
        assertNotNull(metadata);
        assertThat(metadata.policies(), hasKey(policy.name()));
        assertThat(metadata.policies().get(policy.name()).policy(), equalTo(policy));
        verify(mockLogger).info("updating autoscaling policy [{}]", policy.name());
        verifyNoMoreInteractions(mockLogger);

        // ensure that existing policies were otherwise preserved
        for (final Map.Entry<String, AutoscalingPolicyMetadata> entry : currentMetadata.policies().entrySet()) {
            if (entry.getKey().equals(name)) {
                continue;
            }
            assertThat(metadata.policies(), hasKey(entry.getKey()));
            assertThat(metadata.policies().get(entry.getKey()).policy(), equalTo(entry.getValue().policy()));
        }
    }

    public void testNoOpUpdatePolicy() {
        final ClusterState currentState;
        {
            final ClusterState.Builder builder = ClusterState.builder(new ClusterName(randomAlphaOfLength(8)));
            builder.metaData(
                MetaData.builder().putCustom(AutoscalingMetadata.NAME, randomAutoscalingMetadataOfPolicyCount(randomIntBetween(1, 8)))
            );
            currentState = builder.build();
        }
        // randomly put an existing policy
        final AutoscalingMetadata currentMetadata = currentState.metaData().custom(AutoscalingMetadata.NAME);
        final AutoscalingPolicy policy = randomFrom(currentMetadata.policies().values()).policy();
        final Logger mockLogger = mock(Logger.class);
        final ClusterState state = TransportPutAutoscalingPolicyAction.putAutoscalingPolicy(currentState, policy, mockLogger);

        assertThat(state, sameInstance(currentState));
        verify(mockLogger).info("skipping updating autoscaling policy [{}] due to no change in policy", policy.name());
        verifyNoMoreInteractions(mockLogger);
    }

}
