/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.HashedBytesValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.StringValues;
import org.elasticsearch.index.fielddata.ordinals.EmptyOrdinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.util.BytesRefArrayRef;

/**
 */
public class PagedBytesAtomicFieldData implements AtomicFieldData.WithOrdinals<ScriptDocValues.Strings> {

    public static PagedBytesAtomicFieldData empty(int numDocs) {
        return new Empty(numDocs);
    }

    // 0 ordinal in values means no value (its null)
    private final PagedBytes.Reader bytes;
    private final PackedInts.Reader termOrdToBytesOffset;
    protected final Ordinals ordinals;

    private int[] hashes;
    private long size = -1;
    private final long readerBytesSize;

    public PagedBytesAtomicFieldData(PagedBytes.Reader bytes, long readerBytesSize, PackedInts.Reader termOrdToBytesOffset, Ordinals ordinals) {
        this.bytes = bytes;
        this.termOrdToBytesOffset = termOrdToBytesOffset;
        this.ordinals = ordinals;
        this.readerBytesSize = readerBytesSize;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isMultiValued() {
        return ordinals.isMultiValued();
    }

    @Override
    public int getNumDocs() {
        return ordinals.getNumDocs();
    }

    @Override
    public boolean isValuesOrdered() {
        return true;
    }

    @Override
    public long getMemorySizeInBytes() {
        if (size == -1) {
            long size = ordinals.getMemorySizeInBytes();
            // PackedBytes
            size += readerBytesSize;
            // PackedInts
            size += termOrdToBytesOffset.ramBytesUsed();
            this.size = size;
        }
        return size;
    }

    @Override
    public BytesValues.WithOrdinals getBytesValues() {
        return ordinals.isMultiValued() ? new BytesValues.Multi(bytes, termOrdToBytesOffset, ordinals.ordinals()) : new BytesValues.Single(bytes, termOrdToBytesOffset, ordinals.ordinals());
    }

    @Override
    public HashedBytesValues.WithOrdinals getHashedBytesValues() {
        if (hashes == null) {
            int numberOfValues = termOrdToBytesOffset.size();
            int[] hashes = new int[numberOfValues];
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < numberOfValues; i++) {
                bytes.fill(scratch, termOrdToBytesOffset.get(i));
                hashes[i] = scratch.hashCode();
            }
            this.hashes = hashes;
        }
        return ordinals.isMultiValued() ? new HashedBytesValuesWithOrds.Multi(getBytesValues(), hashes) : new HashedBytesValuesWithOrds.Single(getBytesValues(), hashes);
    }

    @Override
    public StringValues.WithOrdinals getStringValues() {
        return StringValues.BytesValuesWrapper.wrap(getBytesValues());
    }

    @Override
    public ScriptDocValues.Strings getScriptValues() {
        return new ScriptDocValues.Strings(getStringValues());
    }

    static abstract class BytesValues extends org.elasticsearch.index.fielddata.BytesValues.WithOrdinals {

        protected final PagedBytes.Reader bytes;
        protected final PackedInts.Reader termOrdToBytesOffset;
        protected final Ordinals.Docs ordinals;

        protected final BytesRef scratch = new BytesRef();

        BytesValues(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, Ordinals.Docs ordinals) {
            super(ordinals);
            this.bytes = bytes;
            this.termOrdToBytesOffset = termOrdToBytesOffset;
            this.ordinals = ordinals;
        }

        @Override
        public Ordinals.Docs ordinals() {
            return this.ordinals;
        }

        @Override
        public BytesRef getValueScratchByOrd(int ord, BytesRef ret) {
            bytes.fill(ret, termOrdToBytesOffset.get(ord));
            return ret;
        }


        static final class Single extends BytesValues {

            private final Iter.Single iter = new Iter.Single();

            Single(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, Ordinals.Docs ordinals) {
                super(bytes, termOrdToBytesOffset, ordinals);
                assert !ordinals.isMultiValued();
            }

            @Override
            public Iter getIter(int docId) {
                int ord = ordinals.getOrd(docId);
                if (ord == 0) return Iter.Empty.INSTANCE;
                bytes.fill(scratch, termOrdToBytesOffset.get(ord));
                return iter.reset(scratch);
            }

        }

        static final class Multi extends BytesValues {

            private final Iter.Multi iter;

            Multi(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, Ordinals.Docs ordinals) {
                super(bytes, termOrdToBytesOffset, ordinals);
                assert ordinals.isMultiValued();
                this.iter = new Iter.Multi(this);
            }

            @Override
            public BytesRefArrayRef getValues(int docId) {
               return getValuesMulti(docId);
            }

            @Override
            public Iter getIter(int docId) {
                return iter.reset(ordinals.getIter(docId));
            }

            @Override
            public void forEachValueInDoc(int docId, ValueInDocProc proc) {
               forEachValueInDocMulti(docId, proc);
            }
        }
    }

    static class Empty extends PagedBytesAtomicFieldData {

        Empty(int numDocs) {
            super(emptyBytes(), 0, new GrowableWriter(1, 2, PackedInts.FASTEST).getMutable(), new EmptyOrdinals(numDocs));
        }

        static PagedBytes.Reader emptyBytes() {
            PagedBytes bytes = new PagedBytes(1);
            bytes.copyUsingLengthPrefix(new BytesRef());
            return bytes.freeze(true);
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public int getNumDocs() {
            return ordinals.getNumDocs();
        }

        @Override
        public boolean isValuesOrdered() {
            return true;
        }

        @Override
        public BytesValues.WithOrdinals getBytesValues() {
            return new BytesValues.WithOrdinals.Empty(ordinals.ordinals());
        }

        @Override
        public HashedBytesValues.WithOrdinals getHashedBytesValues() {
            return new HashedBytesValuesWithOrds.Empty(ordinals);
        }

        @Override
        public StringValues.WithOrdinals getStringValues() {
            return new StringValues.WithOrdinals.Empty((EmptyOrdinals) ordinals);
        }

        @Override
        public ScriptDocValues.Strings getScriptValues() {
            return ScriptDocValues.EMPTY_STRINGS;
        }
    }

}
