/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.core.Releasables;

import java.util.BitSet;

/**
 * Block implementation that stores values in a {@link BooleanBigArrayVector}. Does not take ownership of the given
 * {@link BitArray} and does not adjust circuit breakers to account for it.
 * This class is generated. Do not edit it.
 */
public final class BooleanBigArrayBlock extends AbstractArrayBlock implements BooleanBlock {

    private static final long BASE_RAM_BYTES_USED = 0; // TODO: fix this
    private final BooleanBigArrayVector vector;

    public BooleanBigArrayBlock(
        BitArray values,
        int positionCount,
        int[] firstValueIndexes,
        BitSet nulls,
        MvOrdering mvOrdering,
        BlockFactory blockFactory
    ) {
        this(
            new BooleanBigArrayVector(values, firstValueIndexes == null ? positionCount : firstValueIndexes[positionCount], blockFactory),
            positionCount,
            firstValueIndexes,
            nulls,
            mvOrdering,
            blockFactory
        );
    }

    private BooleanBigArrayBlock(
        BooleanBigArrayVector vector,
        int positionCount,
        int[] firstValueIndexes,
        BitSet nulls,
        MvOrdering mvOrdering,
        BlockFactory blockFactory
    ) {
        super(positionCount, firstValueIndexes, nulls, mvOrdering, blockFactory);
        this.vector = vector;
    }

    @Override
    public BooleanVector asVector() {
        return null;
    }

    @Override
    public boolean getBoolean(int valueIndex) {
        return vector.getBoolean(valueIndex);
    }

    @Override
    public BooleanBlock filter(int... positions) {
        // TODO use reference counting to share the vector
        try (var builder = blockFactory().newBooleanBlockBuilder(positions.length)) {
            for (int pos : positions) {
                if (isNull(pos)) {
                    builder.appendNull();
                    continue;
                }
                int valueCount = getValueCount(pos);
                int first = getFirstValueIndex(pos);
                if (valueCount == 1) {
                    builder.appendBoolean(getBoolean(getFirstValueIndex(pos)));
                } else {
                    builder.beginPositionEntry();
                    for (int c = 0; c < valueCount; c++) {
                        builder.appendBoolean(getBoolean(first + c));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.mvOrdering(mvOrdering()).build();
        }
    }

    @Override
    public ElementType elementType() {
        return ElementType.BOOLEAN;
    }

    @Override
    public BooleanBlock expand() {
        if (firstValueIndexes == null) {
            incRef();
            return this;
        }
        vector.incRef();
        if (nullsMask == null) {
            return vector.asBlock();
        }
        BooleanBigArrayBlock expanded = new BooleanBigArrayBlock(
            vector,
            vector.getPositionCount(),
            null,
            // TODO: we probably need to adjust the breaker before computing the shifted null mask
            shiftNullsToExpandedPositions(),
            MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING,
            blockFactory()
        );
        blockFactory().adjustBreaker(expanded.ramBytesUsedOnlyBlock(), true);
        return expanded;
    }

    private long ramBytesUsedOnlyBlock() {
        return BASE_RAM_BYTES_USED + BlockRamUsageEstimator.sizeOf(firstValueIndexes) + BlockRamUsageEstimator.sizeOfBitSet(nullsMask);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsedOnlyBlock() + RamUsageEstimator.sizeOf(vector);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BooleanBlock that) {
            return BooleanBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BooleanBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[positions="
            + getPositionCount()
            + ", mvOrdering="
            + mvOrdering()
            + ", ramBytesUsed="
            + vector.ramBytesUsed()
            + ']';
    }

    @Override
    public void allowPassingToDifferentDriver() {
        super.allowPassingToDifferentDriver();
        vector.allowPassingToDifferentDriver();
    }

    @Override
    public void closeInternal() {
        blockFactory().adjustBreaker(-ramBytesUsedOnlyBlock(), true);
        Releasables.closeExpectNoException(vector);
    }
}
