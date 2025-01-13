/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaifoundry;

import java.util.Locale;

public enum AzureAiFoundryProvider {
    OPENAI,
    MISTRAL,
    META,
    MICROSOFT_PHI,
    COHERE,
    DATABRICKS,
    NONE;

    public static final String NAME = "azure_ai_foundry_provider";

    public static AzureAiFoundryProvider fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

}
