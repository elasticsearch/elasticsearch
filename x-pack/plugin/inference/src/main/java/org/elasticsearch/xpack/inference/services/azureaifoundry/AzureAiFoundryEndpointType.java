/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaifoundry;

import java.util.Locale;

public enum AzureAiFoundryEndpointType {
    TOKEN,
    REALTIME;

    public static final String NAME = "azure_ai_studio_endpoint_type";

    public static AzureAiFoundryEndpointType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
