/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.inference.TaskType;

public abstract class BaseInferenceActionRequest extends ActionRequest {
    public abstract boolean isStreaming();

    public abstract TaskType getTaskType();

    public abstract String getInferenceEntityId();
}
