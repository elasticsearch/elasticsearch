/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.features;

import java.util.Objects;

/**
 * A feature published by a node.
 *
 * @param id        The feature id. Must be unique in the node.
 * @param assumedInNextMajor
 *              {@code true} if this feature is removed in the next major version,
 *              and so should be assumed to be true for all nodes of the next major version,
 *              or on stateless clusters.
 */
public record NodeFeature(String id, boolean assumedInNextMajor) {

    public NodeFeature {
        Objects.requireNonNull(id);
    }

    public NodeFeature(String id) {
        this(id, false);
    }
}
