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
package org.elasticsearch.index.seqno;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.util.LinkedList;

/**
 * This class generates sequences numbers and keeps track of the so called local checkpoint - the highest number for which
 * all previous seqNo have been processed (including)
 */
public class LocalCheckpointService extends AbstractIndexShardComponent {

    public static String SETTINGS_BIT_ARRAY_CHUNK_SIZE = "index.seq_no.checkpoint.bit_array_chunk_size";

    /** default value for {@link #SETTINGS_BIT_ARRAY_CHUNK_SIZE} */
    final static int DEFAULT_BIT_ARRAY_CHUNK_SIZE = 1024;


    final LinkedList<FixedBitSet> processedSeqNo;
    final int processedSeqNoChunkSize;
    long minSeqNoInProcessSeqNo = 0;

    /** the current local checkpoint, i.e., all seqNo lower&lt;= this number have been completed */
    volatile long checkpoint = -1;

    /** the next available seqNo - used for seqNo generation */
    volatile long nextSeqNo = 0;


    public LocalCheckpointService(ShardId shardId, IndexSettings indexSettings) {
        super(shardId, indexSettings);
        processedSeqNoChunkSize = indexSettings.getSettings().getAsInt(SETTINGS_BIT_ARRAY_CHUNK_SIZE, DEFAULT_BIT_ARRAY_CHUNK_SIZE);
        processedSeqNo = new LinkedList<>();
    }

    /**
     * issue the next sequence number
     **/
    public synchronized long generateSeqNo() {
        return nextSeqNo++;
    }

    /**
     * marks the processing of the given seqNo have been completed
     **/
    public synchronized void markSeqNoAsCompleted(long seqNo) {
        // make sure we track highest seen seqNo
        if (seqNo >= nextSeqNo) {
            nextSeqNo = seqNo + 1;
        }
        if (seqNo <= checkpoint) {
            // this is possible during recover where we might replay an op that was also replicated
            return;
        }
        FixedBitSet bitSet = getBitSetForSeqNo(seqNo);
        int offset = seqNoToBitSetOffset(seqNo);
        bitSet.set(offset);
        if (seqNo == checkpoint + 1) {
            updateCheckpoint();
        }
    }

    /** get's the current check point */
    public long getCheckpoint() {
        return checkpoint;
    }

    /** get's the maximum seqno seen so far */
    public long getMaxSeqNo() {
        return nextSeqNo - 1;
    }

    private void updateCheckpoint() {
        assert Thread.holdsLock(this);
        assert checkpoint - minSeqNoInProcessSeqNo < processedSeqNoChunkSize : "checkpoint to minSeqNoInProcessSeqNo is larger then a bit set";
        assert getBitSetForSeqNo(checkpoint + 1).get(seqNoToBitSetOffset(checkpoint + 1)) : "updateCheckpoint is called but the bit following the checkpoint is not set";
        assert getBitSetForSeqNo(checkpoint + 1) == processedSeqNo.getFirst() : "checkpoint + 1 doesn't point to the first bit set";
        // keep it simple for now, get the checkpoint one by one. in the future we can optimize and read words
        FixedBitSet current = processedSeqNo.getFirst();
        do {
            checkpoint++;
            // the checkpoint always falls in the first bit set or just before. If it falls
            // on the last bit of the current bit set, we can clean it.
            if (checkpoint == minSeqNoInProcessSeqNo + processedSeqNoChunkSize - 1) {
                processedSeqNo.pop();
                minSeqNoInProcessSeqNo += processedSeqNoChunkSize;
                assert checkpoint - minSeqNoInProcessSeqNo < processedSeqNoChunkSize;
                current = processedSeqNo.peekFirst();
            }
        } while (current != null && current.get(seqNoToBitSetOffset(checkpoint + 1)));
    }

    private FixedBitSet getBitSetForSeqNo(long seqNo) {
        assert Thread.holdsLock(this);
        assert seqNo >= minSeqNoInProcessSeqNo;
        int bitSetOffset = ((int) (seqNo - minSeqNoInProcessSeqNo)) / processedSeqNoChunkSize;
        while (bitSetOffset >= processedSeqNo.size()) {
            processedSeqNo.add(new FixedBitSet(processedSeqNoChunkSize));
        }
        return processedSeqNo.get(bitSetOffset);
    }


    /** maps the given seqNo to a position in the bit set returned by {@link #getBitSetForSeqNo} */
    private int seqNoToBitSetOffset(long seqNo) {
        assert Thread.holdsLock(this);
        assert seqNo >= minSeqNoInProcessSeqNo;
        return ((int) (seqNo - minSeqNoInProcessSeqNo)) % processedSeqNoChunkSize;
    }
}
