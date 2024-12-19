/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OrdinalsGroupingOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.SourceOperator.SourceOperatorFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.TestBlockFactory;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.carrotsearch.randomizedtesting.generators.RandomNumbers.randomIntBetween;
import static java.util.stream.Collectors.joining;
import static org.apache.lucene.tests.util.LuceneTestCase.createTempDir;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.DOC_VALUES;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.NONE;

public class TestPhysicalOperationProviders extends AbstractPhysicalOperationProviders {
    private final List<IndexPage> indexPages;

    private TestPhysicalOperationProviders(List<IndexPage> indexPages, AnalysisRegistry analysisRegistry) {
        super(analysisRegistry);
        this.indexPages = indexPages;
    }

    public static TestPhysicalOperationProviders create(List<IndexPage> indexPages) throws IOException {
        return new TestPhysicalOperationProviders(indexPages, createAnalysisRegistry());
    }

    public record IndexPage(String index, Page page, List<String> columnNames) {
        int columnIndex(String columnName) {
            return IntStream.range(0, columnNames.size())
                .filter(i -> columnNames.get(i).equals(columnName))
                .findFirst()
                .orElseThrow(() -> new EsqlIllegalArgumentException("Cannot find column named [{}] in {}", columnName, columnNames));
        }

        boolean isColumnMissing(String s) {
            return IntStream.range(0, columnNames.size()).noneMatch(i -> columnNames.get(i).equals(s));
        }
    }

    private static AnalysisRegistry createAnalysisRegistry() throws IOException {
        return new AnalysisModule(
            TestEnvironment.newEnvironment(
                Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build()
            ),
            List.of(new MachineLearning(Settings.EMPTY), new CommonAnalysisPlugin()),
            new StablePluginsRegistry()
        ).getAnalysisRegistry();
    }

    @Override
    public PhysicalOperation fieldExtractPhysicalOperation(FieldExtractExec fieldExtractExec, PhysicalOperation source) {
        Layout.Builder layout = source.layout.builder();
        PhysicalOperation op = source;
        for (Attribute attr : fieldExtractExec.attributesToExtract()) {
            layout.append(attr);
            op = op.with(
                new TestFieldExtractOperatorFactory(
                    attr,
                    PlannerUtils.extractPreference(fieldExtractExec.docValuesAttributes().contains(attr))
                ),
                layout.build()
            );
        }
        return op;
    }

    @Override
    public PhysicalOperation sourcePhysicalOperation(EsQueryExec esQueryExec, LocalExecutionPlannerContext context) {
        Layout.Builder layout = new Layout.Builder();
        layout.append(esQueryExec.output());
        return PhysicalOperation.fromSource(new TestSourceOperatorFactory(), layout.build());
    }

    @Override
    public Operator.OperatorFactory ordinalGroupingOperatorFactory(
        PhysicalOperation source,
        AggregateExec aggregateExec,
        List<GroupingAggregator.Factory> aggregatorFactories,
        Attribute attrSource,
        ElementType groupElementType,
        LocalExecutionPlannerContext context
    ) {
        int channelIndex = source.layout.numberOfChannels();
        return new TestOrdinalsGroupingAggregationOperatorFactory(
            channelIndex,
            aggregatorFactories,
            groupElementType,
            context.bigArrays(),
            attrSource
        );
    }

    private class TestSourceOperator extends SourceOperator {
        private int index = 0;
        private final DriverContext driverContext;

        TestSourceOperator(DriverContext driverContext) {
            this.driverContext = driverContext;
        }

        @Override
        public Page getOutput() {
            var pageIndex = indexPages.get(index);
            var page = pageIndex.page;
            BlockFactory blockFactory = driverContext.blockFactory();
            DocVector docVector = new DocVector(
                blockFactory.newConstantIntVector(index, page.getPositionCount()),
                blockFactory.newConstantIntVector(0, page.getPositionCount()),
                blockFactory.newIntArrayVector(IntStream.range(0, page.getPositionCount()).toArray(), page.getPositionCount()),
                true
            );
            var block = docVector.asBlock();
            index++;
            return new Page(block);
        }

        @Override
        public boolean isFinished() {
            return index == indexPages.size();
        }

        @Override
        public void finish() {
            index = indexPages.size();
        }

        @Override
        public void close() {

        }
    }

    private class TestSourceOperatorFactory implements SourceOperatorFactory {

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new TestSourceOperator(driverContext);
        }

        @Override
        public String describe() {
            return "TestSourceOperator";
        }
    }

    private class TestFieldExtractOperator implements Operator {
        private final Attribute attribute;
        private Page lastPage;
        boolean finished;
        private final MappedFieldType.FieldExtractPreference extractPreference;

        TestFieldExtractOperator(Attribute attr, MappedFieldType.FieldExtractPreference extractPreference) {
            this.attribute = attr;
            this.extractPreference = extractPreference;
        }

        @Override
        public void addInput(Page page) {
            lastPage = page.appendBlock(getBlock(page.getBlock(0), attribute, extractPreference));
        }

        @Override
        public Page getOutput() {
            Page l = lastPage;
            lastPage = null;
            return l;
        }

        @Override
        public boolean isFinished() {
            return finished && lastPage == null;
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public boolean needsInput() {
            return lastPage == null;
        }

        @Override
        public void close() {

        }
    }

    private class TestFieldExtractOperatorFactory implements Operator.OperatorFactory {
        private final Operator op;
        private final Attribute attribute;

        TestFieldExtractOperatorFactory(Attribute attr, MappedFieldType.FieldExtractPreference extractPreference) {
            this.op = new TestFieldExtractOperator(attr, extractPreference);
            this.attribute = attr;
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return op;
        }

        @Override
        public String describe() {
            return "TestFieldExtractOperator(" + attribute.name() + ")";
        }
    }

    private Block getBlock(DocBlock docBlock, Attribute attribute, MappedFieldType.FieldExtractPreference extractPreference) {
        if (attribute instanceof UnsupportedAttribute) {
            return docBlock.blockFactory().newConstantNullBlock(docBlock.getPositionCount());
        }
        return extractBlockForColumn(
            docBlock,
            attribute.dataType(),
            extractPreference,
            attribute instanceof FieldAttribute fa && fa.field() instanceof MultiTypeEsField multiTypeEsField
                ? (shardDoc, blockCopier) -> getBlockForMultiType(shardDoc, multiTypeEsField, blockCopier)
                : (shardDoc, blockCopier) -> extractBlockForSingleDoc(shardDoc, attribute.name(), blockCopier)
        );
    }

    private Block getBlockForMultiType(DocBlock shardDoc, MultiTypeEsField multiTypeEsField, TestBlockCopier blockCopier) {
        var shard = shardDoc.asVector().shards().getInt(0);
        var indexPage = indexPages.get(shard);
        var conversion = (AbstractConvertFunction) multiTypeEsField.getConversionExpressionForIndex(indexPage.index);
        Supplier<Block> nulls = () -> shardDoc.blockFactory().newConstantNullBlock(shardDoc.getPositionCount());
        if (conversion == null) {
            return nulls.get();
        }
        var field = (FieldAttribute) conversion.field();
        return indexPage.isColumnMissing(field.fieldName())
            ? nulls.get()
            : TypeConverter.fromConvertFunction(conversion).convert(extractBlockForSingleDoc(shardDoc, field.fieldName(), blockCopier));
    }

    private Block extractBlockForSingleDoc(DocBlock docBlock, String columnName, TestBlockCopier blockCopier) {
        var shard = docBlock.asVector().shards().getInt(0);
        var indexPage = indexPages.get(shard);
        int columnIndex = indexPage.columnIndex(columnName);
        var originalData = indexPage.page.getBlock(columnIndex);
        return blockCopier.copyBlock(originalData);
    }

    private static Iterable<DocBlock> splitByShards(DocBlock docBlock) {
        var indicesByShard = new LinkedHashMap<Integer, List<Integer>>();
        DocVector vector = docBlock.asVector();
        for (int i = 0; i < docBlock.getPositionCount(); i++) {
            int shardId = vector.shards().getInt(i);
            indicesByShard.computeIfAbsent(shardId, k -> new ArrayList<>()).add(i);
        }
        var result = new ArrayList<DocBlock>(indicesByShard.size());
        for (var indices : indicesByShard.values()) {
            result.add(vector.filter(indices.stream().mapToInt(Integer::intValue).toArray()).asBlock());
        }
        return result;
    }

    private class TestHashAggregationOperator extends HashAggregationOperator {

        private final Attribute attribute;

        TestHashAggregationOperator(
            List<GroupingAggregator.Factory> aggregators,
            Supplier<BlockHash> blockHash,
            Attribute attribute,
            DriverContext driverContext
        ) {
            super(aggregators, blockHash, driverContext);
            this.attribute = attribute;
        }

        @Override
        protected Page wrapPage(Page page) {
            return page.appendBlock(getBlock(page.getBlock(0), attribute, NONE));
        }
    }

    /**
     * Pretends to be the {@link OrdinalsGroupingOperator} but always delegates to the
     * {@link HashAggregationOperator}.
     */
    private class TestOrdinalsGroupingAggregationOperatorFactory implements Operator.OperatorFactory {
        private final int groupByChannel;
        private final List<GroupingAggregator.Factory> aggregators;
        private final ElementType groupElementType;
        private final BigArrays bigArrays;
        private final Attribute attribute;

        TestOrdinalsGroupingAggregationOperatorFactory(
            int channelIndex,
            List<GroupingAggregator.Factory> aggregatorFactories,
            ElementType groupElementType,
            BigArrays bigArrays,
            Attribute attribute
        ) {
            this.groupByChannel = channelIndex;
            this.aggregators = aggregatorFactories;
            this.groupElementType = groupElementType;
            this.bigArrays = bigArrays;
            this.attribute = attribute;
        }

        @Override
        public Operator get(DriverContext driverContext) {
            Random random = Randomness.get();
            int pageSize = random.nextBoolean() ? randomIntBetween(random, 1, 16) : randomIntBetween(random, 1, 10 * 1024);
            return new TestHashAggregationOperator(
                aggregators,
                () -> BlockHash.build(
                    List.of(new BlockHash.GroupSpec(groupByChannel, groupElementType)),
                    driverContext.blockFactory(),
                    pageSize,
                    false
                ),
                attribute,
                driverContext
            );
        }

        @Override
        public String describe() {
            return "TestHashAggregationOperator(mode = "
                + "<not-needed>"
                + ", aggs = "
                + aggregators.stream().map(Describable::describe).collect(joining(", "))
                + ")";
        }
    }

    private Block extractBlockForColumn(
        DocBlock docBlock,
        DataType dataType,
        MappedFieldType.FieldExtractPreference extractPreference,
        BiFunction<DocBlock, TestBlockCopier, Block> extractBlock
    ) {
        BlockFactory blockFactory = docBlock.blockFactory();
        boolean mapToDocValues = shouldMapToDocValues(dataType, extractPreference);
        TestBlockCopier blockCopier = mapToDocValues
            ? TestSpatialPointStatsBlockCopier.create(docBlock.asVector().docs(), dataType)
            : new TestBlockCopier(docBlock.asVector().docs());
        try (
            Block.Builder blockBuilder = mapToDocValues
                ? blockFactory.newLongBlockBuilder(docBlock.getPositionCount())
                : PlannerUtils.blockBuilder(dataType, docBlock.getPositionCount(), TestBlockFactory.getNonBreakingInstance())
        ) {
            for (DocBlock shardDoc : splitByShards(docBlock)) {
                try {
                    Block nextBlock = extractBlock.apply(shardDoc, blockCopier);
                    blockBuilder.copyFrom(nextBlock, 0, nextBlock.getPositionCount());
                    nextBlock.close();
                } finally {
                    shardDoc.close();
                }
            }
            var result = blockBuilder.build();
            assert result.getPositionCount() == docBlock.getPositionCount()
                : "Expected " + docBlock.getPositionCount() + " rows, got " + result.getPositionCount();
            return result;
        }
    }

    private boolean shouldMapToDocValues(DataType dataType, MappedFieldType.FieldExtractPreference extractPreference) {
        return extractPreference == DOC_VALUES && DataType.isSpatialPoint(dataType);
    }

    private static class TestBlockCopier {

        protected final IntVector docIndices;

        private TestBlockCopier(IntVector docIndices) {
            this.docIndices = docIndices;
        }

        protected Block copyBlock(Block originalData) {
            try (
                Block.Builder builder = originalData.elementType()
                    .newBlockBuilder(docIndices.getPositionCount(), TestBlockFactory.getNonBreakingInstance())
            ) {
                for (int c = 0; c < docIndices.getPositionCount(); c++) {
                    int doc = docIndices.getInt(c);
                    builder.copyFrom(originalData, doc, doc + 1);
                }
                return builder.build();
            }
        }
    }

    /**
     * geo_point and cartesian_point are normally loaded as WKT from source, but for aggregations we can load them as doc-values
     * which are encoded Long values. This class is used to convert the test loaded WKB into encoded longs for the aggregators.
     * TODO: We need a different solution to support geo_shape and cartesian_shape
     */
    private abstract static class TestSpatialPointStatsBlockCopier extends TestBlockCopier {

        private TestSpatialPointStatsBlockCopier(IntVector docIndices) {
            super(docIndices);
        }

        protected abstract long encode(BytesRef wkb);

        @Override
        protected Block copyBlock(Block originalData) {
            BytesRef scratch = new BytesRef(100);
            BytesRefBlock bytesRefBlock = (BytesRefBlock) originalData;
            try (LongBlock.Builder builder = bytesRefBlock.blockFactory().newLongBlockBuilder(docIndices.getPositionCount())) {
                for (int c = 0; c < docIndices.getPositionCount(); c++) {
                    int doc = docIndices.getInt(c);
                    int count = bytesRefBlock.getValueCount(doc);
                    int i = bytesRefBlock.getFirstValueIndex(doc);
                    if (count == 0) {
                        builder.appendNull();
                    } else {
                        if (count > 1) {
                            builder.beginPositionEntry();
                        }
                        for (int v = 0; v < count; v++) {
                            builder.appendLong(encode(bytesRefBlock.getBytesRef(i + v, scratch)));
                        }
                        if (count > 1) {
                            builder.endPositionEntry();
                        }
                    }
                }
                return builder.build();
            }
        }

        private static TestSpatialPointStatsBlockCopier create(IntVector docIndices, DataType dataType) {
            Function<BytesRef, Long> encoder = switch (dataType) {
                case GEO_POINT -> SpatialCoordinateTypes.GEO::wkbAsLong;
                case CARTESIAN_POINT -> SpatialCoordinateTypes.CARTESIAN::wkbAsLong;
                default -> throw new IllegalArgumentException("Unsupported spatial data type: " + dataType);
            };
            return new TestSpatialPointStatsBlockCopier(docIndices) {
                @Override
                protected long encode(BytesRef wkb) {
                    return encoder.apply(wkb);
                }
            };
        }
    }
}
