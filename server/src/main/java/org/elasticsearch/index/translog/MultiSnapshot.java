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

package org.elasticsearch.index.translog;

import com.carrotsearch.hppc.LongLongHashMap;
import org.elasticsearch.Assertions;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

/**
 * A snapshot composed out of multiple snapshots
 */
final class MultiSnapshot implements Translog.Snapshot {
    private final TranslogSnapshot[] translogs;
    private final int totalOperations;
    private final Closeable onClose;
    private int index = 0;
    private final LongLongHashMap seqNoToTerm; // for assertion purpose

    /**
     * Creates a new point in time snapshot of the given snapshots. Those snapshots are always iterated in-order.
     */
    MultiSnapshot(TranslogSnapshot[] translogs, Closeable onClose) {
        this.translogs = translogs;
        this.totalOperations = Arrays.stream(translogs).mapToInt(TranslogSnapshot::totalOperations).sum();
        this.onClose = onClose;
        this.seqNoToTerm = Assertions.ENABLED ? new LongLongHashMap() : null;
    }

    @Override
    public int totalOperations() {
        return totalOperations;
    }

    @Override
    public int skippedOperations() {
        return Arrays.stream(translogs).mapToInt(TranslogSnapshot::skippedOperations).sum();
    }

    @Override
    public Translog.Operation next() throws IOException {
        for (; index < translogs.length; index ++) {
            final TranslogSnapshot current = translogs[index];
            Translog.Operation op;
            while ((op = current.next()) != null) {
                assert assertSameOperation(op);
                return op;
            }
        }
        return null;
    }

    private boolean assertSameOperation(Translog.Operation op) {
        final long existingTerm = seqNoToTerm.put(op.seqNo(), op.primaryTerm());
        assert existingTerm == 0 || existingTerm == op.primaryTerm() :
            "Operation [" + op + "] was associated with a different primary term [" + existingTerm + "]";
        return true;
    }

    @Override
    public void close() throws IOException {
        onClose.close();
    }
}
