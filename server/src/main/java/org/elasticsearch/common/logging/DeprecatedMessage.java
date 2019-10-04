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

package org.elasticsearch.common.logging;

import org.elasticsearch.common.Strings;

/**
 * A logger message used by {@link DeprecationLogger}.
 * Carries x-opaque-id field if provided in the headers. Will populate the x-opaque-id field in JSON logs.
 */
public class DeprecatedMessage  {
    private static final String X_OPAQUE_ID_FIELD_NAME = "x-opaque-id";

    public static ESLogMessage of(String xOpaqueId, String messagePattern, Object... args){
        if (Strings.isNullOrEmpty(xOpaqueId)) {
            return ESLogMessage.of(messagePattern,args);
        }

        return ESLogMessage.of(messagePattern,args)
                           .with(X_OPAQUE_ID_FIELD_NAME, xOpaqueId);
    }
}
