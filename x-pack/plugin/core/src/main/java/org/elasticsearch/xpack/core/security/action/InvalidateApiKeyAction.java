/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.Writeable;

/**
 * ActionType for invalidating API key
 */
public final class InvalidateApiKeyAction extends ActionType<InvalidateApiKeyResponse> {

    public static final String NAME = "cluster:admin/xpack/security/api_key/invalidate";
    public static final InvalidateApiKeyAction INSTANCE = new InvalidateApiKeyAction();

    private InvalidateApiKeyAction() {
        super(NAME);
    }

    @Override
    public Writeable.Reader<InvalidateApiKeyResponse> getResponseReader() {
        return InvalidateApiKeyResponse::new;
    }
}
