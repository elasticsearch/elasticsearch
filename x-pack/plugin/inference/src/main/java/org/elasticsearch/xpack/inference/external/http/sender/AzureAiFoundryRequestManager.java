/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryModel;

import java.util.Objects;

public abstract class AzureAiFoundryRequestManager extends BaseRequestManager {

    protected AzureAiFoundryRequestManager(ThreadPool threadPool, AzureAiFoundryModel model) {
        super(threadPool, model.getInferenceEntityId(), AzureAiFoundryRequestManager.RateLimitGrouping.of(model), model.rateLimitSettings());
    }

    record RateLimitGrouping(int targetHashcode) {
        public static AzureAiFoundryRequestManager.RateLimitGrouping of(AzureAiFoundryModel model) {
            Objects.requireNonNull(model);

            return new AzureAiFoundryRequestManager.RateLimitGrouping(model.target().hashCode());
        }
    }
}
