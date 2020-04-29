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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude.LongFilter;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds.BucketOrdsEnum;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms.Bucket;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

public class LongTermsAggregator extends TermsAggregator {

    protected final ValuesSource.Numeric valuesSource;
    protected final LongKeyedBucketOrds bucketOrds;
    private boolean showTermDocCountError;
    private LongFilter longFilter;

    public LongTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource, DocValueFormat format,
            BucketOrder order, BucketCountThresholds bucketCountThresholds, SearchContext aggregationContext, Aggregator parent,
            SubAggCollectionMode subAggCollectMode, boolean showTermDocCountError, IncludeExclude.LongFilter longFilter,
            boolean collectsFromSingleBucket, Map<String, Object> metadata) throws IOException {
        super(name, factories, aggregationContext, parent, bucketCountThresholds, order, format, subAggCollectMode, metadata);
        this.valuesSource = valuesSource;
        this.showTermDocCountError = showTermDocCountError;
        this.longFilter = longFilter;
        bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), collectsFromSingleBucket);
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    protected SortedNumericDocValues getValues(ValuesSource.Numeric valuesSource, LeafReaderContext ctx) throws IOException {
        return valuesSource.longValues(ctx);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        final SortedNumericDocValues values = getValues(valuesSource, ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        final long val = values.nextValue();
                        if (previous != val || i == 0) {
                            if ((longFilter == null) || (longFilter.accept(val))) {
                                long bucketOrdinal = bucketOrds.add(owningBucketOrd, val);
                                if (bucketOrdinal < 0) { // already seen
                                    bucketOrdinal = -1 - bucketOrdinal;
                                    collectExistingBucket(sub, doc, bucketOrdinal);
                                } else {
                                    collectBucket(sub, doc, bucketOrdinal);
                                }
                            }

                            previous = val;
                        }
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(int owningBucketOrdsToCollect) throws IOException {
        try (LongHash survivingOrds = new LongHash(owningBucketOrdsToCollect * bucketCountThresholds.getShardSize(), context.bigArrays())) {
            LongTerms.Bucket[][] topBuckets = new LongTerms.Bucket[owningBucketOrdsToCollect][];
            long[] otherDocCounts = new long[owningBucketOrdsToCollect];
            for (int owningBucketOrd = 0; owningBucketOrd < owningBucketOrdsToCollect; owningBucketOrd++) {
                long bucketsInOrd = bucketOrds.bucketsInOrd(owningBucketOrd);
                if (bucketCountThresholds.getMinDocCount() == 0 && (InternalOrder.isCountDesc(order) == false ||
                        bucketsInOrd < bucketCountThresholds.getRequiredSize())) {
                    // we need to fill-in the blanks
                    for (LeafReaderContext ctx : context.searcher().getTopReaderContext().leaves()) {
                        final SortedNumericDocValues values = getValues(valuesSource, ctx);
                        for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                            if (values.advanceExact(docId)) {
                                final int valueCount = values.docValueCount();
                                for (int i = 0; i < valueCount; ++i) {
                                    long value = values.nextValue();
                                    if (longFilter == null || longFilter.accept(value)) {
                                        bucketOrds.add(owningBucketOrd, value);
                                    }
                                }
                            }
                        }
                    }
                    bucketsInOrd = bucketOrds.bucketsInOrd(owningBucketOrd);
                }
    
                final int size = (int) Math.min(bucketsInOrd, bucketCountThresholds.getShardSize());
                BucketPriorityQueue<LongTerms.Bucket> ordered = new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
                LongTerms.Bucket spare = null;
                BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrd);
                while (ordsEnum.next()) {
                    if (spare == null) {
                        spare = new LongTerms.Bucket(0, 0, null, showTermDocCountError, 0, format);
                    }
                    spare.term = ordsEnum.value();
                    spare.docCount = bucketDocCount(ordsEnum.ord());
                    otherDocCounts[owningBucketOrd] += spare.docCount;
                    spare.bucketOrd = ordsEnum.ord();
                    if (bucketCountThresholds.getShardMinDocCount() <= spare.docCount) {
                        spare = ordered.insertWithOverflow(spare);
                        if (spare == null) {
                            consumeBucketsAndMaybeBreak(1);
                        }
                    }
                }
    
                // Get the top buckets
                LongTerms.Bucket[] list = topBuckets[owningBucketOrd] = new LongTerms.Bucket[ordered.size()];
                for (int i = ordered.size() - 1; i >= 0; --i) {
                    final LongTerms.Bucket bucket = ordered.pop();
                    bucket.bucketOrd = survivingOrds.add(bucket.bucketOrd);
                    if (bucket.bucketOrd < 0) {
                        bucket.bucketOrd = 1 - bucket.bucketOrd;
                    }
                    list[i] = bucket;
                    otherDocCounts[owningBucketOrd] -= bucket.docCount;
                }
            }

            runDeferredCollections(survivingOrds);
            InternalAggregations[] bucketAggs = buildSubAggsForBuckets(survivingOrds.size());

            InternalAggregation[] result = new InternalAggregation[owningBucketOrdsToCollect];
            for (int owningBucketOrd = 0; owningBucketOrd < owningBucketOrdsToCollect; owningBucketOrd++) {
                for (int i = 0; i < topBuckets[0].length; i++) {
                    topBuckets[owningBucketOrd][i].aggregations = bucketAggs[(int) survivingOrds.find(topBuckets[0][i].bucketOrd)];
                    topBuckets[owningBucketOrd][i].docCountError = 0;
                }

                result[owningBucketOrd] = buildResult(otherDocCounts[owningBucketOrd], Arrays.asList(topBuckets[owningBucketOrd]));
            }
            return result;
        }
    }

    protected InternalAggregation buildResult(long otherDocCount, List<Bucket> buckets) {
        return new LongTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(),
            metadata(), format, bucketCountThresholds.getShardSize(), showTermDocCountError, otherDocCount,
            buckets, 0);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new LongTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(),
                metadata(), format, bucketCountThresholds.getShardSize(), showTermDocCountError, 0, emptyList(), 0);
    }

    @Override
    public void doClose() {
        super.doClose();
        Releasables.close(bucketOrds);
    }
}
