/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.indexlifecycle;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.TimedRequest;

import java.util.Arrays;
import java.util.Objects;

public class SetIndexLifecyclePolicyRequest extends TimedRequest {

    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
    private String policy;

    public SetIndexLifecyclePolicyRequest() {
    }

    public SetIndexLifecyclePolicyRequest(String policy, String... indices) {
        if (indices == null) {
            throw new IllegalArgumentException("indices cannot be null");
        }
        if (policy == null) {
            throw new IllegalArgumentException("policy cannot be null");
        }
        this.indices = indices;
        this.policy = policy;
    }

    public SetIndexLifecyclePolicyRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public String[] indices() {
        return indices;
    }

    public SetIndexLifecyclePolicyRequest policy(String policy) {
        this.policy = policy;
        return this;
    }

    public String policy() {
        return policy;
    }

    public void indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), indicesOptions, policy);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SetIndexLifecyclePolicyRequest other = (SetIndexLifecyclePolicyRequest) obj;
        return Objects.deepEquals(indices, other.indices) &&
            Objects.equals(indicesOptions, other.indicesOptions) &&
            Objects.equals(policy, other.policy);
    }

}
