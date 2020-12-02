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

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class CumulativeSumPipelineAggregator extends PipelineAggregator {
    private final DocValueFormat formatter;

    CumulativeSumPipelineAggregator(String name, String[] bucketsPaths, DocValueFormat formatter,
                                    Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends InternalMultiBucketAggregation.InternalBucket>
            parentAggregate = (InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends
            InternalMultiBucketAggregation.InternalBucket>) aggregation;

        InternalAggregation internalAggregation = null;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = parentAggregate.getBuckets();

        double sum = 0;
        Map<Integer, List<InternalAggregation>> aggregationMap = new HashMap<>();
        for (int i = 0; i < buckets.size(); ++i) {
            InternalMultiBucketAggregation.InternalBucket bucket = buckets.get(i);

            Double thisBucketValue = resolveBucketValue(parentAggregate, bucket, bucketsPaths()[0], GapPolicy.INSERT_ZEROS);
            if (thisBucketValue != null && thisBucketValue.isInfinite() == false && thisBucketValue.isNaN() == false) {
                sum += thisBucketValue;
            }

            List<InternalAggregation> aggregate = StreamSupport.stream(bucket.getAggregations().spliterator(), false)
                .map((p) -> (InternalAggregation) p)
                .collect(Collectors.toList());
            aggregate.add(new InternalSimpleValue(name(), sum, formatter, metadata()));
            aggregationMap.put(i, aggregate);
        }

        //FIXME: kludgy. assess interfaces, probably create another interface/factory/helper
        if (parentAggregate instanceof HistogramFactory) {
            HistogramFactory factory = (HistogramFactory) parentAggregate;
            List<Bucket> newBuckets = new ArrayList<>(buckets.size());
            for (Map.Entry<Integer, List<InternalAggregation>> entry : aggregationMap.entrySet()) {
                Bucket bucket = buckets.get(entry.getKey());
                Bucket newBucket = factory.createBucket(factory.getKey(bucket), bucket.getDocCount(), InternalAggregations.from(entry.getValue()));
                newBuckets.add(newBucket);
            }
            internalAggregation = factory.createAggregation(newBuckets);
        } else if (parentAggregate instanceof LongTerms) {
            LongTerms factory = (LongTerms) parentAggregate;
            List<LongTerms.Bucket> newBuckets = new ArrayList<>(buckets.size());
            for (Map.Entry<Integer, List<InternalAggregation>> entry : aggregationMap.entrySet()) {
                LongTerms.Bucket newBucket = factory.createBucket(InternalAggregations.from(entry.getValue()), (LongTerms.Bucket) buckets.get(entry.getKey()));
                newBuckets.add(newBucket);
            }
            internalAggregation = factory.create(newBuckets);
        } else if (parentAggregate instanceof DoubleTerms) {
            DoubleTerms factory = (DoubleTerms) parentAggregate;
            List<DoubleTerms.Bucket> newBuckets = new ArrayList<>(buckets.size());
            for (Map.Entry<Integer, List<InternalAggregation>> entry : aggregationMap.entrySet()) {
                DoubleTerms.Bucket newBucket = factory.createBucket(InternalAggregations.from(entry.getValue()), (DoubleTerms.Bucket) buckets.get(entry.getKey()));
                newBuckets.add(newBucket);
            }
            internalAggregation = factory.create(newBuckets);
        } else if (parentAggregate instanceof StringTerms) {
            StringTerms factory = (StringTerms) parentAggregate;
            List<StringTerms.Bucket> newBuckets = new ArrayList<>(buckets.size());
            for (Map.Entry<Integer, List<InternalAggregation>> entry : aggregationMap.entrySet()) {
                StringTerms.Bucket newBucket = factory.createBucket(InternalAggregations.from(entry.getValue()), (StringTerms.Bucket) buckets.get(entry.getKey()));
                newBuckets.add(newBucket);
            }
            internalAggregation = factory.create(newBuckets);
        }

        //FIXME: should we do a last ifblock to address null?
        return internalAggregation;
    }


}
