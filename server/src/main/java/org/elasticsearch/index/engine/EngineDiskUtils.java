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

package org.elasticsearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public final class EngineDiskUtils {

    private EngineDiskUtils() {
    }

    public static void createEmpty(final Directory dir, final Path translogPath, final ShardId shardId) throws IOException {
        try (IndexWriter writer = newIndexWriter(true, dir)) {
            final String translogUuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId);
            final Map<String, String> map = new HashMap<>();
            map.put(Translog.TRANSLOG_GENERATION_KEY, "1");
            map.put(Translog.TRANSLOG_UUID_KEY, translogUuid);
            map.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID());
            map.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            map.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            map.put(InternalEngine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, "-1");
            updateCommitData(writer, map);
        }
    }


    public static void bootstrapNewHistoryFromLuceneIndex(final Directory dir, final Path translogPath, final ShardId shardId)
        throws IOException {
        try (IndexWriter writer = newIndexWriter(false, dir)) {
            final Map<String, String> userData = getUserData(writer);
            final long maxSeqNo = Long.parseLong(userData.get(SequenceNumbers.MAX_SEQ_NO));
            final String translogUuid = Translog.createEmptyTranslog(translogPath, maxSeqNo, shardId);
            final Map<String, String> map = new HashMap<>();
            map.put(Translog.TRANSLOG_GENERATION_KEY, "1");
            map.put(Translog.TRANSLOG_UUID_KEY, translogUuid);
            map.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID());
            map.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(maxSeqNo));
            updateCommitData(writer, map);
        }
    }

    public static void createNewTranslog(final Directory dir, final Path translogPath, long initialGlobalCheckpoint, final ShardId shardId)
        throws IOException {
        try (IndexWriter writer = newIndexWriter(false, dir)) {
            final String translogUuid = Translog.createEmptyTranslog(translogPath, initialGlobalCheckpoint, shardId);
            final Map<String, String> map = new HashMap<>();
            map.put(Translog.TRANSLOG_GENERATION_KEY, "1");
            map.put(Translog.TRANSLOG_UUID_KEY, translogUuid);
            updateCommitData(writer, map);
        }
    }

    private static void updateCommitData(IndexWriter writer, Map<String, String> keysToUpdate) throws IOException {
        List<IndexCommit> commits = DirectoryReader.listCommits(writer.getDirectory());
        if (commits.size() != 1) {
            throw new UnsupportedOperationException("can only update commit data if there's a single commit, found [" +
                commits.size() + "]");
        }

        final Map<String, String> userData = getUserData(writer);
        userData.putAll(keysToUpdate);
        writer.setLiveCommitData(userData.entrySet());
        writer.commit();
    }

    private static Map<String, String> getUserData(IndexWriter writer) {
        final Map<String, String> userData = new HashMap<>();
        writer.getLiveCommitData().forEach(e -> userData.put(e.getKey(), e.getValue()));
        return userData;
    }

    private static IndexWriter newIndexWriter(final boolean existing, final Directory dir) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(null)
            .setCommitOnClose(false)
            // we don't want merges to happen here - we call maybe merge on the engine
            // later once we stared it up otherwise we would need to wait for it here
            // we also don't specify a codec here and merges should use the engines for this index
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(existing ? IndexWriterConfig.OpenMode.APPEND : IndexWriterConfig.OpenMode.CREATE);
        return new IndexWriter(dir, iwc);
    }
}
