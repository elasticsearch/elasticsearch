/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaifoundry;

import java.util.Locale;

public enum AzureAiFoundryDeploymentType {
    AZURE_AI_MODEL_INFERENCE_SERVICE,
    SERVERLESS_API;

    public static AzureAiFoundryDeploymentType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
