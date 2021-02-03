/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.engine;

import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;

import static java.util.Map.entry;

/** a class the returns dynamic information with respect to the last commit point of this shard */
public final class CommitStats implements Writeable, ToXContentFragment {

    private final Map<String, String> userData;
    private final long generation;
    private final String id; // lucene commit id in base 64;
    private final int numDocs;

    public CommitStats(SegmentInfos segmentInfos) {
        // clone the map to protect against concurrent changes
        userData = Map.copyOf(segmentInfos.getUserData());
        // lucene calls the current generation, last generation.
        generation = segmentInfos.getLastGeneration();
        id = Base64.getEncoder().encodeToString(segmentInfos.getId());
        numDocs = Lucene.getNumDocs(segmentInfos);
    }

    CommitStats(StreamInput in) throws IOException {
        final int length = in.readVInt();
        final var entries = new ArrayList<Map.Entry<String, String>>(length);
        for (int i = length; i > 0; i--) {
            entries.add(entry(in.readString(), in.readString()));
        }
        userData = Maps.ofEntries(entries);
        generation = in.readLong();
        id = in.readOptionalString();
        numDocs = in.readInt();
    }

    public static CommitStats readOptionalCommitStatsFrom(StreamInput in) throws IOException {
        return in.readOptionalWriteable(CommitStats::new);
    }


    public Map<String, String> getUserData() {
        return userData;
    }

    public long getGeneration() {
        return generation;
    }

    /** base64 version of the commit id (see {@link SegmentInfos#getId()} */
    public String getId() {
        return id;
    }

    /**
     * Returns the number of documents in the in this commit
     */
    public int getNumDocs() {
        return numDocs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(userData.size());
        for (Map.Entry<String, String> entry : userData.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        out.writeLong(generation);
        out.writeOptionalString(id);
        out.writeInt(numDocs);
    }

    static final class Fields {
        static final String GENERATION = "generation";
        static final String USER_DATA = "user_data";
        static final String ID = "id";
        static final String COMMIT = "commit";
        static final String NUM_DOCS = "num_docs";

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.COMMIT);
        builder.field(Fields.ID, id);
        builder.field(Fields.GENERATION, generation);
        builder.field(Fields.USER_DATA, userData);
        builder.field(Fields.NUM_DOCS, numDocs);
        builder.endObject();
        return builder;
    }
}
