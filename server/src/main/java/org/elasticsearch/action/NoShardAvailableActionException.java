/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class NoShardAvailableActionException extends ElasticsearchException {

    public NoShardAvailableActionException(ShardId shardId) {
        this(shardId, null);
    }

    public NoShardAvailableActionException(ShardId shardId, String msg) {
        this(shardId, msg, null);
    }

    public NoShardAvailableActionException(ShardId shardId, String msg, Throwable cause) {
        super(msg, cause);
        setShard(shardId);
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }

    public NoShardAvailableActionException(StreamInput in) throws IOException{
        super(in);
    }
}
