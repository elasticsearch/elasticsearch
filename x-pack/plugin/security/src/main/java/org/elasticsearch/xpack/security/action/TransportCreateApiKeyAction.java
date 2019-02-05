/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

/**
 * Implementation of the action needed to create an API key
 */
public final class TransportCreateApiKeyAction extends HandledTransportAction<CreateApiKeyRequest, CreateApiKeyResponse> {

    private final ApiKeyService apiKeyService;
    private final SecurityContext securityContext;

    @Inject
    public TransportCreateApiKeyAction(Settings settings, ThreadPool threadPool, TransportService transportService,
            ActionFilters actionFilters, ApiKeyService apiKeyService, SecurityContext context,
            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, CreateApiKeyAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                CreateApiKeyRequest::new);
        this.apiKeyService = apiKeyService;
        this.securityContext = context;
    }

    @Override
    protected void doExecute(CreateApiKeyRequest request, ActionListener<CreateApiKeyResponse> listener) {
        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
        } else {
            apiKeyService.createApiKey(authentication, request, listener);
        }
    }
}
