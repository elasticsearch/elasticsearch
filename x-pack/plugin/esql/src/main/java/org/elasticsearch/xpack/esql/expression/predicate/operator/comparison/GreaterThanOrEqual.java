/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.Negatable;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation;

import java.time.ZoneId;
import java.util.Map;

public class GreaterThanOrEqual extends EsqlBinaryComparison implements Negatable<EsqlBinaryComparison> {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "GreaterThanOrEqual",
        EsqlBinaryComparison::readFrom
    );

    private static final Map<DataType, EsqlArithmeticOperation.BinaryEvaluator> evaluatorMap = Map.ofEntries(
        Map.entry(DataType.INTEGER, GreaterThanOrEqualIntsEvaluator.Factory::new),
        Map.entry(DataType.DOUBLE, GreaterThanOrEqualDoublesEvaluator.Factory::new),
        Map.entry(DataType.LONG, GreaterThanOrEqualLongsEvaluator.Factory::new),
        Map.entry(DataType.UNSIGNED_LONG, GreaterThanOrEqualLongsEvaluator.Factory::new),
        Map.entry(DataType.DATETIME, GreaterThanOrEqualLongsEvaluator.Factory::new),
        Map.entry(DataType.KEYWORD, GreaterThanOrEqualKeywordsEvaluator.Factory::new),
        Map.entry(DataType.TEXT, GreaterThanOrEqualKeywordsEvaluator.Factory::new),
        Map.entry(DataType.VERSION, GreaterThanOrEqualKeywordsEvaluator.Factory::new),
        Map.entry(DataType.IP, GreaterThanOrEqualKeywordsEvaluator.Factory::new)
    );

    public GreaterThanOrEqual(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryComparisonOperation.GTE, evaluatorMap);
    }

    public GreaterThanOrEqual(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, BinaryComparisonOperation.GTE, zoneId, evaluatorMap);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<GreaterThanOrEqual> info() {
        return NodeInfo.create(this, GreaterThanOrEqual::new, left(), right(), zoneId());
    }

    @Override
    protected GreaterThanOrEqual replaceChildren(Expression newLeft, Expression newRight) {
        return new GreaterThanOrEqual(source(), newLeft, newRight, zoneId());
    }

    @Override
    public LessThanOrEqual swapLeftAndRight() {
        return new LessThanOrEqual(source(), right(), left(), zoneId());
    }

    @Override
    public LessThan negate() {
        return new LessThan(source(), left(), right(), zoneId());
    }

    @Override
    public EsqlBinaryComparison reverse() {
        return new LessThanOrEqual(source(), left(), right(), zoneId());
    }

    @Evaluator(extraName = "Ints")
    static boolean processInts(int lhs, int rhs) {
        return lhs >= rhs;
    }

    @Evaluator(extraName = "Longs")
    static boolean processLongs(long lhs, long rhs) {
        return lhs >= rhs;
    }

    @Evaluator(extraName = "Doubles")
    static boolean processDoubles(double lhs, double rhs) {
        return lhs >= rhs;
    }

    @Evaluator(extraName = "Keywords")
    static boolean processKeywords(BytesRef lhs, BytesRef rhs) {
        return lhs.compareTo(rhs) >= 0;
    }
}
