/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanArrayBlock;
import org.elasticsearch.compute.data.BooleanArrayVector;
import org.elasticsearch.compute.data.BooleanBigArrayBlock;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefArrayBlock;
import org.elasticsearch.compute.data.BytesRefArrayVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleArrayBlock;
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.DoubleBigArrayBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayBlock;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBigArrayBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class BlockBenchmark {
    public static final int NUM_BLOCKS_PER_ITERATION = 1024;
    public static final int BLOCK_TOTAL_POSITIONS = 8096;

    private static final double MV_PERCENTAGE = 0.3;
    private static final double NULL_PERCENTAGE = 0.1;
    private static final int MAX_MV_ELEMENTS = 100;
    private static final int MAX_BYTES_REF_LENGTH = 255;

    private static final Random random = new Random();

    static {
        // Smoke test all the expected values and force loading subclasses more like prod
        try {
            int totalPositions = 10;
            long[] actualCheckSums = new long[NUM_BLOCKS_PER_ITERATION];

            for (String paramString : getParamValues("dataTypeAndBlockKind")) {
                String[] params = paramString.split("/");
                String dataType = params[0];
                String blockKind = params[1];

                BenchmarkBlocks data = buildBlocks(dataType, blockKind, totalPositions);
                int[][] traversalOrders = createTraversalOrders(data.blocks, false);
                run(dataType, data, traversalOrders, actualCheckSums);
                assertCheckSums(data, actualCheckSums);
            }
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    private static String[] getParamValues(String parameterName) throws NoSuchFieldException {
        return BlockBenchmark.class.getField(parameterName).getAnnotationsByType(Param.class)[0].value();
    }

    private record BenchmarkBlocks(Block[] blocks, long[] checkSums) {};

    private static BenchmarkBlocks buildBlocks(String dataType, String blockKind, int totalPositions) {
        Block[] blocks = new Block[NUM_BLOCKS_PER_ITERATION];
        long[] checkSums = new long[NUM_BLOCKS_PER_ITERATION];

        switch (dataType) {
            case "boolean" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    boolean[] values = new boolean[totalPositions];
                    for (int i = 0; i < totalPositions; i++) {
                        values[i] = random.nextBoolean();
                    }

                    switch (blockKind) {
                        // TODO: add also BigArrayBlocks
                        case "array" -> {
                            blocks[blockIndex] = new BooleanArrayBlock(
                                values,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
                            );
                        }
                        case "array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);

                            blocks[blockIndex] = new BooleanArrayBlock(
                                values,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED
                            );
                        }
                        case "big-array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);
                            BitArray valuesBigArray = new BitArray(totalPositions, BigArrays.NON_RECYCLING_INSTANCE);
                            for (int i = 0; i < values.length; i++) {
                                if (values[i]) {
                                    valuesBigArray.set(i);
                                }
                            }

                            blocks[blockIndex] = new BooleanBigArrayBlock(
                                valuesBigArray,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED,
                                BlockFactory.getNonBreakingInstance()
                            );
                        }
                        case "vector" -> {
                            // TODO: add also BigArrayVectors
                            BooleanVector vector = new BooleanArrayVector(values, totalPositions);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        default -> {
                            throw new IllegalStateException("illegal block kind [" + blockKind + "]");
                        }
                    }

                    BooleanBlock block = (BooleanBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeBooleanCheckSum(block, IntStream.range(0, block.getPositionCount()).toArray());
                }
            }
            case "BytesRef" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    BytesRefArray values = new BytesRefArray(totalPositions, BigArrays.NON_RECYCLING_INSTANCE);
                    byte[] bytes;
                    for (int i = 0; i < totalPositions; i++) {
                        bytes = new byte[random.nextInt(MAX_BYTES_REF_LENGTH)];
                        random.nextBytes(bytes);
                        values.append(new BytesRef(bytes));
                    }

                    switch (blockKind) {
                        case "array" -> {
                            blocks[blockIndex] = new BytesRefArrayBlock(
                                values,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
                            );
                        }
                        case "array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);

                            blocks[blockIndex] = new BytesRefArrayBlock(
                                values,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED
                            );
                        }
                        case "vector" -> {
                            BytesRefVector vector = new BytesRefArrayVector(values, totalPositions);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        default -> {
                            throw new IllegalStateException("illegal block kind [" + blockKind + "]");
                        }
                    }

                    BytesRefBlock block = (BytesRefBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeBytesRefCheckSum(block, IntStream.range(0, block.getPositionCount()).toArray());
                }
            }
            case "double" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    double[] values = new double[totalPositions];
                    for (int i = 0; i < totalPositions; i++) {
                        values[i] = random.nextDouble() * 1000000.0;
                    }

                    switch (blockKind) {
                        case "array" -> {
                            blocks[blockIndex] = new DoubleArrayBlock(
                                values,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
                            );
                        }
                        case "array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);

                            blocks[blockIndex] = new DoubleArrayBlock(
                                values,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED
                            );
                        }
                        case "big-array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);
                            DoubleArray valuesBigArray = BlockFactory.getNonBreakingInstance()
                                .bigArrays()
                                .newDoubleArray(totalPositions, false);
                            for (int i = 0; i < values.length; i++) {
                                valuesBigArray.set(i, values[i]);
                            }

                            blocks[blockIndex] = new DoubleBigArrayBlock(
                                valuesBigArray,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED,
                                BlockFactory.getNonBreakingInstance()
                            );
                        }
                        case "vector" -> {
                            DoubleVector vector = new DoubleArrayVector(values, totalPositions);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        default -> {
                            throw new IllegalStateException("illegal block kind [" + blockKind + "]");
                        }
                    }

                    DoubleBlock block = (DoubleBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeDoubleCheckSum(block, IntStream.range(0, block.getPositionCount()).toArray());
                }
            }
            case "int" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    int[] values = new int[totalPositions];
                    for (int i = 0; i < totalPositions; i++) {
                        values[i] = random.nextInt();
                    }

                    switch (blockKind) {
                        case "array" -> {
                            blocks[blockIndex] = new IntArrayBlock(
                                values,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
                            );
                        }
                        case "array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);

                            blocks[blockIndex] = new IntArrayBlock(
                                values,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED
                            );
                        }
                        case "big-array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);
                            IntArray valuesBigArray = BlockFactory.getNonBreakingInstance().bigArrays().newIntArray(totalPositions, false);
                            for (int i = 0; i < values.length; i++) {
                                valuesBigArray.set(i, values[i]);
                            }

                            blocks[blockIndex] = new IntBigArrayBlock(
                                valuesBigArray,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED,
                                BlockFactory.getNonBreakingInstance()
                            );
                        }
                        case "vector" -> {
                            IntVector vector = new IntArrayVector(values, totalPositions);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        default -> {
                            throw new IllegalStateException("illegal block kind [" + blockKind + "]");
                        }
                    }

                    IntBlock block = (IntBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeIntCheckSum(block, IntStream.range(0, block.getPositionCount()).toArray());
                }
            }
            case "long" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    long[] values = new long[totalPositions];
                    for (int i = 0; i < totalPositions; i++) {
                        values[i] = random.nextLong();
                    }

                    switch (blockKind) {
                        case "array" -> {
                            blocks[blockIndex] = new LongArrayBlock(
                                values,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
                            );
                        }
                        case "array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);

                            blocks[blockIndex] = new LongArrayBlock(
                                values,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED
                            );
                        }
                        case "big-array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);
                            LongArray valuesBigArray = BlockFactory.getNonBreakingInstance()
                                .bigArrays()
                                .newLongArray(totalPositions, false);
                            for (int i = 0; i < values.length; i++) {
                                valuesBigArray.set(i, values[i]);
                            }

                            blocks[blockIndex] = new LongBigArrayBlock(
                                valuesBigArray,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED,
                                BlockFactory.getNonBreakingInstance()
                            );
                        }
                        case "vector" -> {
                            LongVector vector = new LongArrayVector(values, totalPositions);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        default -> {
                            throw new IllegalStateException("illegal block kind [" + blockKind + "]");
                        }
                    }

                    LongBlock block = (LongBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeLongCheckSum(block, IntStream.range(0, block.getPositionCount()).toArray());
                }
            }
            default -> {
                throw new IllegalStateException("illegal data type [" + dataType + "]");
            }
        }

        return new BenchmarkBlocks(blocks, checkSums);
    }

    private static int[][] createTraversalOrders(Block[] blocks, boolean randomized) {
        int[][] orders = new int[blocks.length][];

        for (int i = 0; i < blocks.length; i++) {
            IntStream positionsStream = IntStream.range(0, blocks[i].getPositionCount());

            if (randomized) {
                List<Integer> positions = new java.util.ArrayList<>(positionsStream.boxed().toList());
                Collections.shuffle(positions, random);
                orders[i] = positions.stream().mapToInt(x -> x).toArray();
            } else {
                orders[i] = positionsStream.toArray();
            }
        }

        return orders;
    }

    private static int[] randomFirstValueIndexes(int totalPositions) {
        ArrayList<Integer> firstValueIndexes = new ArrayList<>();
        firstValueIndexes.add(0);

        int currentPosition = 0;
        int nextPosition;
        while (currentPosition < totalPositions) {
            if (random.nextDouble() < MV_PERCENTAGE) {
                nextPosition = Math.min(currentPosition + 1 + random.nextInt(MAX_MV_ELEMENTS), totalPositions);
            } else {
                nextPosition = currentPosition + 1;
            }
            firstValueIndexes.add(nextPosition);
            currentPosition = nextPosition;
        }

        return firstValueIndexes.stream().mapToInt(x -> x).toArray();
    }

    private static BitSet randomNulls(int positionCount) {
        BitSet nulls = new BitSet(positionCount);
        for (int i = 0; i < positionCount; i++) {
            if (random.nextDouble() < NULL_PERCENTAGE) {
                nulls.set(i);
            }
        }

        return nulls;
    }

    private static void run(String dataType, BenchmarkBlocks data, int[][] traversalOrders, long[] resultCheckSums) {
        switch (dataType) {
            case "boolean" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    BooleanBlock block = (BooleanBlock) data.blocks[blockIndex];

                    resultCheckSums[blockIndex] = computeBooleanCheckSum(block, traversalOrders[blockIndex]);
                }
            }
            case "BytesRef" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    BytesRefBlock block = (BytesRefBlock) data.blocks[blockIndex];

                    resultCheckSums[blockIndex] = computeBytesRefCheckSum(block, traversalOrders[blockIndex]);
                }
            }
            case "double" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    DoubleBlock block = (DoubleBlock) data.blocks[blockIndex];

                    resultCheckSums[blockIndex] = computeDoubleCheckSum(block, traversalOrders[blockIndex]);
                }
            }
            case "int" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    IntBlock block = (IntBlock) data.blocks[blockIndex];

                    resultCheckSums[blockIndex] = computeIntCheckSum(block, traversalOrders[blockIndex]);
                }
            }
            case "long" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    LongBlock block = (LongBlock) data.blocks[blockIndex];

                    resultCheckSums[blockIndex] = computeLongCheckSum(block, traversalOrders[blockIndex]);
                }
            }
            default -> {
                throw new IllegalStateException();
            }
        }
    }

    private static void assertCheckSums(BenchmarkBlocks data, long[] actualCheckSums) {
        for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
            if (actualCheckSums[blockIndex] != data.checkSums[blockIndex]) {
                throw new AssertionError("checksums do not match for block [" + blockIndex + "]");
            }
        }
    }

    private static long computeBooleanCheckSum(BooleanBlock block, int[] traversalOrder) {
        long sum = 0;

        for (int position : traversalOrder) {
            if (block.isNull(position)) {
                continue;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            for (int i = start; i < end; i++) {
                sum += block.getBoolean(i) ? 1 : 0;
            }
        }

        return sum;
    }

    private static long computeBytesRefCheckSum(BytesRefBlock block, int[] traversalOrder) {
        long sum = 0;
        BytesRef currentValue = new BytesRef();

        for (int position : traversalOrder) {
            if (block.isNull(position)) {
                continue;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            for (int i = start; i < end; i++) {
                block.getBytesRef(i, currentValue);
                sum += currentValue.length > 0 ? currentValue.bytes[0] : 0;
            }
        }

        return sum;
    }

    private static long computeDoubleCheckSum(DoubleBlock block, int[] traversalOrder) {
        double sum = 0;

        for (int position : traversalOrder) {
            if (block.isNull(position)) {
                continue;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            for (int i = start; i < end; i++) {
                sum += block.getDouble(i);
            }
        }

        return (long) sum;
    }

    private static long computeIntCheckSum(IntBlock block, int[] traversalOrder) {
        int sum = 0;

        for (int position : traversalOrder) {
            if (block.isNull(position)) {
                continue;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            for (int i = start; i < end; i++) {
                sum += block.getInt(i);
            }
        }

        return sum;
    }

    private static long computeLongCheckSum(LongBlock block, int[] traversalOrder) {
        long sum = 0;

        for (int position : traversalOrder) {
            if (block.isNull(position)) {
                continue;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            for (int i = start; i < end; i++) {
                sum += block.getLong(i);
            }
        }

        return sum;
    }

    private static boolean isRandom(String accessType) {
        return accessType.equalsIgnoreCase("random");
    }

    // We could also consider DocBlocks/DocVectors.
    @Param(
        {
            "boolean/array",
            "boolean/array-multivalue-null",
            "boolean/big-array-multivalue-null",
            "boolean/vector",
            "BytesRef/array",
            "BytesRef/array-multivalue-null",
            "BytesRef/vector",
            "double/array",
            "double/array-multivalue-null",
            "double/big-array-multivalue-null",
            "double/vector",
            "int/array",
            "int/array-multivalue-null",
            "int/big-array-multivalue-null",
            "int/vector",
            "long/array",
            "long/array-multivalue-null",
            "long/big-array-multivalue-null",
            "long/vector" }
    )
    public String dataTypeAndBlockKind;

    // @Param({ "sequential", "random" })
    @Param({ "sequential" })
    public String accessType;

    private BenchmarkBlocks data;

    private int[][] traversalOrders;

    private final long[] actualCheckSums = new long[NUM_BLOCKS_PER_ITERATION];

    @Setup
    public void setup() {
        String[] params = dataTypeAndBlockKind.split("/");
        String dataType = params[0];
        String blockKind = params[1];

        data = buildBlocks(dataType, blockKind, BLOCK_TOTAL_POSITIONS);
        traversalOrders = createTraversalOrders(data.blocks, isRandom(accessType));
    }

    @Benchmark
    @OperationsPerInvocation(NUM_BLOCKS_PER_ITERATION * BLOCK_TOTAL_POSITIONS)
    public void run() {
        String[] params = dataTypeAndBlockKind.split("/");
        String dataType = params[0];

        run(dataType, data, traversalOrders, actualCheckSums);
    }

    @TearDown(Level.Iteration)
    public void assertCheckSums() {
        assertCheckSums(data, actualCheckSums);
    }
}
