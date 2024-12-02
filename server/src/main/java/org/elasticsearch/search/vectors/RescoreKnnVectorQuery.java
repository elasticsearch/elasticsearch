/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.index.mapper.vectors.VectorSimilarityByteValueSource;
import org.elasticsearch.index.mapper.vectors.VectorSimilarityFloatValueSource;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Wraps an internal query to rescore the results using a similarity function over the original, non-quantized vectors of a vector field
 */
public class RescoreKnnVectorQuery extends Query implements ProfilingQuery {
    private final String fieldName;
    private final byte[] byteTarget;
    private final float[] floatTarget;
    private final VectorSimilarityFunction vectorSimilarityFunction;
    private final Integer k;
    private final Query innerQuery;

    private long vectorOpsCount;

    public RescoreKnnVectorQuery(
        String fieldName,
        byte[] byteTarget,
        VectorSimilarityFunction vectorSimilarityFunction,
        Integer k,
        Query innerQuery
    ) {
        this.fieldName = fieldName;
        this.byteTarget = byteTarget;
        this.floatTarget = null;
        this.vectorSimilarityFunction = vectorSimilarityFunction;
        this.k = k;
        this.innerQuery = innerQuery;
    }

    public RescoreKnnVectorQuery(
        String fieldName,
        float[] floatTarget,
        VectorSimilarityFunction vectorSimilarityFunction,
        Integer k,
        Query innerQuery
    ) {
        this.fieldName = fieldName;
        this.byteTarget = null;
        this.floatTarget = floatTarget;
        this.vectorSimilarityFunction = vectorSimilarityFunction;
        this.k = k;
        this.innerQuery = innerQuery;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        assert byteTarget == null ^ floatTarget == null : "Either byteTarget or floatTarget must be set";

        final DoubleValuesSource valueSource;
        if (byteTarget != null) {
            valueSource = new VectorSimilarityByteValueSource(fieldName, byteTarget, vectorSimilarityFunction);
        } else {
            valueSource = new VectorSimilarityFloatValueSource(fieldName, floatTarget, vectorSimilarityFunction);
        }
        FunctionScoreQuery functionScoreQuery = new FunctionScoreQuery(innerQuery, valueSource);
        Query query = searcher.rewrite(functionScoreQuery);

        if (k == null) {
            // No need to calculate top k - let the request size limit the results
            return query;
        }

        // Retrieve top k documents from the rescored query
        TopDocs topDocs = searcher.search(query, k);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        int[] docIds = new int[scoreDocs.length];
        float[] scores = new float[scoreDocs.length];
        for (int i = 0; i < scoreDocs.length; i++) {
            docIds[i] = scoreDocs[i].doc;
            scores[i] = scoreDocs[i].score;
        }

        vectorOpsCount = scoreDocs.length;

        return new KnnScoreDocQuery(docIds, scores, searcher.getIndexReader());
    }

    public Query innerQuery() {
        return innerQuery;
    }

    public Integer k() {
        return k;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        if (innerQuery instanceof ProfilingQuery profilingQuery) {
            profilingQuery.profile(queryProfiler);
        }
        queryProfiler.addVectorOpsCount(vectorOpsCount);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RescoreKnnVectorQuery that = (RescoreKnnVectorQuery) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.deepEquals(byteTarget, that.byteTarget)
            && Objects.deepEquals(floatTarget, that.floatTarget)
            && vectorSimilarityFunction == that.vectorSimilarityFunction
            && Objects.equals(k, that.k)
            && Objects.equals(innerQuery, that.innerQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, Arrays.hashCode(byteTarget), Arrays.hashCode(floatTarget), vectorSimilarityFunction, k, innerQuery);
    }

    @Override
    public String toString(String field) {
        final StringBuilder sb = new StringBuilder("KnnRescoreVectorQuery{");
        sb.append("fieldName='").append(fieldName).append('\'');
        if (byteTarget != null) {
            sb.append(", byteTarget=").append(Arrays.toString(byteTarget));
        } else {
            sb.append(", floatTarget=").append(Arrays.toString(floatTarget));
        }
        sb.append(", vectorSimilarityFunction=").append(vectorSimilarityFunction);
        sb.append(", k=").append(k);
        sb.append(", vectorQuery=").append(innerQuery);
        sb.append('}');
        return sb.toString();
    }
}
