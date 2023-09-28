/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.comparators.DoubleComparator;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * Comparator source for double values.
 */
public class DoubleValuesComparatorSource extends IndexFieldData.XFieldComparatorSource {

    final IndexNumericFieldData indexFieldData;

    public DoubleValuesComparatorSource(
        IndexNumericFieldData indexFieldData,
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        Nested nested
    ) {
        super(missingValue, sortMode, nested);
        this.indexFieldData = indexFieldData;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.DOUBLE;
    }

    private SortedNumericDoubleValues getValues(LeafReaderContext context) throws IOException {
        return indexFieldData.load(context).getDoubleValues();
    }

    protected NumericDoubleValues getNumericDoubleDocValues(
        LeafReaderContext context,
        double missingValue,
        CheckedSupplier<SortedNumericDoubleValues, IOException> docValuesSupplier
    ) throws IOException {
        final SortedNumericDoubleValues values = docValuesSupplier.get();
        if (nested == null) {
            return FieldData.replaceMissing(sortMode.select(values), missingValue);
        } else {
            final BitSet rootDocs = nested.rootDocs(context);
            final DocIdSetIterator innerDocs = nested.innerDocs(context);
            final int maxChildren = nested.getNestedSort() != null ? nested.getNestedSort().getMaxChildren() : Integer.MAX_VALUE;
            return sortMode.select(values, missingValue, rootDocs, innerDocs, context.reader().maxDoc(), maxChildren);
        }
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, boolean enableSkipping, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final double dMissingValue = (Double) missingObject(missingValue, reversed);
        // NOTE: it's important to pass null as a missing value in the constructor so that
        // the comparator doesn't check docsWithField since we replace missing values in select()
        return new DoubleComparator(numHits, null, null, reversed, false) {
            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                return new DoubleLeafComparator(context) {
                    @Override
                    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                        return getNumericDoubleDocValues(context, dMissingValue, () -> getValues(context)).getRawDoubleValues();
                    }
                };
            }
        };
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        return new BucketedSort.ForDoubles(bigArrays, sortOrder, format, bucketSize, extra) {
            private final double dMissingValue = (Double) missingObject(missingValue, sortOrder == SortOrder.DESC);

            @Override
            public Leaf forLeaf(LeafReaderContext leafReaderContext) throws IOException {
                return new Leaf(leafReaderContext) {
                    private final NumericDoubleValues docValues = getNumericDoubleDocValues(
                        leafReaderContext,
                        dMissingValue,
                        () -> DoubleValuesComparatorSource.this.getValues(leafReaderContext)
                    );
                    private double docValue;

                    @Override
                    protected boolean advanceExact(int doc) throws IOException {
                        if (docValues.advanceExact(doc)) {
                            docValue = docValues.doubleValue();
                            return true;
                        }
                        return false;
                    }

                    @Override
                    protected double docValue() {
                        return docValue;
                    }
                };
            }
        };
    }
}
