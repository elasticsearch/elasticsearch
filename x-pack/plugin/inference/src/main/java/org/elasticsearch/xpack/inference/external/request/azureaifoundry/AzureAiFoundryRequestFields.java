/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureaifoundry;

public final class AzureAiFoundryRequestFields {
    public static final String API_KEY_HEADER = "api-key";
    public static final String MESSAGES_ARRAY = "messages";
    public static final String MESSAGE_CONTENT = "content";
    public static final String ROLE = "role";
    public static final String USER_ROLE = "user";
    public static final String STREAM = "stream";

    private AzureAiFoundryRequestFields() {}
}
