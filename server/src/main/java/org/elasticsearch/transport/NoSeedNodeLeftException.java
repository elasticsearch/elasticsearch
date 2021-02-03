/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Thrown after failed to connect to all seed nodes of the remote cluster.
 */
public class NoSeedNodeLeftException extends ElasticsearchException {

    public NoSeedNodeLeftException(String clusterName) {
        super("no seed node left for cluster: [" + clusterName + "]");
    }

    public NoSeedNodeLeftException(StreamInput in) throws IOException {
        super(in);
    }
}
