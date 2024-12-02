/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * DoubleValuesSource that is used to calculate scores according to a similarity function for a KnnByteVectorField, using the
 * original vector values stored in the index
 */
public class VectorSimilarityByteValueSource extends DoubleValuesSource {

    private final String field;
    private final byte[] target;
    private final VectorSimilarityFunction vectorSimilarityFunction;

    public VectorSimilarityByteValueSource(String field, byte[] target, VectorSimilarityFunction vectorSimilarityFunction) {
        this.field = field;
        this.target = target;
        this.vectorSimilarityFunction = vectorSimilarityFunction;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
        final LeafReader reader = ctx.reader();

        ByteVectorValues vectorValues = reader.getByteVectorValues(field);
        final KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();

        return new DoubleValues() {
            private int docId = -1;

            @Override
            public double doubleValue() throws IOException {
                return vectorSimilarityFunction.compare(target, vectorValues.vectorValue(docId));
            }

            @Override
            public boolean advanceExact(int doc) throws IOException {
                docId = doc;
                return doc >= iterator.docID() && iterator.docID() != DocIdSetIterator.NO_MORE_DOCS && iterator.advance(doc) == doc;
            }
        };
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, Arrays.hashCode(target), vectorSimilarityFunction);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VectorSimilarityByteValueSource that = (VectorSimilarityByteValueSource) o;
        return Objects.equals(field, that.field)
            && Objects.deepEquals(target, that.target)
            && vectorSimilarityFunction == that.vectorSimilarityFunction;
    }

    @Override
    public String toString() {
        return "VectorSimilarityByteValueSource(" + field + ", " + Arrays.toString(target) + ", " + vectorSimilarityFunction + ")";
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }
}
