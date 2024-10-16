/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;

import java.util.List;

import static org.elasticsearch.ExceptionsHelper.stackTrace;

record ErrorState(
    String namespace,
    Long version,
    Boolean reprocessSameVersion,
    List<String> errors,
    ReservedStateErrorMetadata.ErrorKind errorKind
) {
    ErrorState(
        String namespace,
        ReservedStateVersionParameters versionParameters,
        Exception e,
        ReservedStateErrorMetadata.ErrorKind errorKind
    ) {
        this(namespace, versionParameters.version().version(), versionParameters.reprocessSameVersion(), List.of(stackTrace(e)), errorKind);
    }

    ErrorState(
        String namespace,
        ReservedStateVersionParameters versionParameters,
        List<String> errors,
        ReservedStateErrorMetadata.ErrorKind errorKind
    ) {
        this(namespace, versionParameters.version().version(), versionParameters.reprocessSameVersion(), errors, errorKind);
    }

    ErrorState(String namespace, Long version, Exception e, ReservedStateErrorMetadata.ErrorKind errorKind) {
        this(namespace, version, false, List.of(stackTrace(e)), errorKind);
    }

    public String toString() {
        return String.join(", ", errors());
    }
}
