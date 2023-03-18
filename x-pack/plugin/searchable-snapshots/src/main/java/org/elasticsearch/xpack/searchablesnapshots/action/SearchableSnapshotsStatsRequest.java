/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.ClusterStatsLevel;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Objects;

public class SearchableSnapshotsStatsRequest extends BroadcastRequest<SearchableSnapshotsStatsRequest> {

    private ClusterStatsLevel level = ClusterStatsLevel.INDICES;

    SearchableSnapshotsStatsRequest(StreamInput in) throws IOException {
        super(in);
    }

    public SearchableSnapshotsStatsRequest(String... indices) {
        super(indices);
    }

    public SearchableSnapshotsStatsRequest(String[] indices, IndicesOptions indicesOptions) {
        super(indices, indicesOptions);
    }

    public void level(ClusterStatsLevel level) {
        this.level = Objects.requireNonNull(level, "level must not be null");
    }

    public ClusterStatsLevel level() {
        return level;
    }
}
