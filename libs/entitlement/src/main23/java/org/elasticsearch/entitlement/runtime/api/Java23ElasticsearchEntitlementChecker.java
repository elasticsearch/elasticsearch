/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.api;

import org.elasticsearch.entitlement.bridge.Java23EntitlementChecker;

public class Java23ElasticsearchEntitlementChecker extends ElasticsearchEntitlementChecker implements Java23EntitlementChecker {
    @Override
    public void checkSystemExit(Class<?> callerClass, int status) {
        throw new RuntimeException("system exit not allowed in java 23");
    }
}
