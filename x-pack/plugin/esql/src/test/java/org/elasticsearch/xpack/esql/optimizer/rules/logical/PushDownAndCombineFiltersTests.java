/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.FOUR;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.ONE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.THREE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TWO;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOrEqualOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.rlike;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.wildcardLike;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class PushDownAndCombineFiltersTests extends ESTestCase {

    public void testCombineFilters() {
        EsRelation relation = relation();
        GreaterThan conditionA = greaterThanOf(getFieldAttribute("a"), ONE);
        LessThan conditionB = lessThanOf(getFieldAttribute("b"), TWO);

        Filter fa = new Filter(EMPTY, relation, conditionA);
        Filter fb = new Filter(EMPTY, fa, conditionB);

        assertEquals(new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)), new PushDownAndCombineFilters().apply(fb));
    }

    public void testCombineFiltersLikeRLike() {
        EsRelation relation = relation();
        RLike conditionA = rlike(getFieldAttribute("a"), "foo");
        WildcardLike conditionB = wildcardLike(getFieldAttribute("b"), "bar");

        Filter fa = new Filter(EMPTY, relation, conditionA);
        Filter fb = new Filter(EMPTY, fa, conditionB);

        assertEquals(new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)), new PushDownAndCombineFilters().apply(fb));
    }

    public void testPushDownFilter() {
        EsRelation relation = relation();
        GreaterThan conditionA = greaterThanOf(getFieldAttribute("a"), ONE);
        LessThan conditionB = lessThanOf(getFieldAttribute("b"), TWO);

        Filter fa = new Filter(EMPTY, relation, conditionA);
        List<FieldAttribute> projections = singletonList(getFieldAttribute("b"));
        EsqlProject keep = new EsqlProject(EMPTY, fa, projections);
        Filter fb = new Filter(EMPTY, keep, conditionB);

        Filter combinedFilter = new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB));
        assertEquals(new EsqlProject(EMPTY, combinedFilter, projections), new PushDownAndCombineFilters().apply(fb));
    }

    // ... | eval a_renamed = a, a_renamed_twice = a_renamed, non_pushable = pow(a, 2)
    // | where a_renamed > 1 and a_renamed_twice < 2 and non_pushable < 4
    // ->
    // ... | where a > 1 and a < 2 | eval a_renamed = a, a_renamed_twice = a_renamed, non_pushable = pow(a, 2) | where non_pushable < 4
    // TODO: More test cases. Also add cases for pushing down past RENAME (Project) while we're at it.
    public void testPushDownFilterOnAliasInEval() {
        FieldAttribute a = getFieldAttribute("a");
        EsRelation relation = relation(List.of(a));

        Alias aRenamed = new Alias(EMPTY, "a_renamed", a);
        Alias aRenamedTwice = new Alias(EMPTY, "a_renamed_twice", aRenamed.toAttribute());
        Alias nonPushable = new Alias(EMPTY, "non_pushable", new Pow(EMPTY, a, TWO));
        Eval eval = new Eval(EMPTY, relation, List.of(aRenamed, aRenamedTwice, nonPushable));

        GreaterThan aRenamedGreaterThanOne = greaterThanOf(aRenamed.toAttribute(), ONE);
        LessThan aRenamedTwiceLessThanTwo = lessThanOf(aRenamedTwice.toAttribute(), TWO);
        LessThan nonPushableLessThanFour = lessThanOf(nonPushable, FOUR);
        Filter filter = new Filter(
            EMPTY,
            eval,
            Predicates.combineAnd(List.of(aRenamedGreaterThanOne, aRenamedTwiceLessThanTwo, nonPushableLessThanFour))
        );

        LogicalPlan optimized = new PushDownAndCombineFilters().apply(filter);

        Filter optimizedFilter = as(optimized, Filter.class);
        assertEquals(optimizedFilter.condition(), nonPushableLessThanFour);
        Eval optimizedEval = as(optimizedFilter.child(), Eval.class);
        assertEquals(optimizedEval.fields(), eval.fields());
        Filter pushedFilter = as(optimizedEval.child(), Filter.class);
        GreaterThan aGreaterThanOne = greaterThanOf(a, ONE);
        LessThan aLessThanTwo = lessThanOf(a, TWO);
        assertEquals(pushedFilter.condition(), Predicates.combineAnd(List.of(aGreaterThanOne, aLessThanTwo)));
        EsRelation optimizedRelation = as(pushedFilter.child(), EsRelation.class);
        assertEquals(optimizedRelation, relation);
    }

    public void testPushDownLikeRlikeFilter() {
        EsRelation relation = relation();
        org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLike conditionA = rlike(getFieldAttribute("a"), "foo");
        WildcardLike conditionB = wildcardLike(getFieldAttribute("b"), "bar");

        Filter fa = new Filter(EMPTY, relation, conditionA);
        List<FieldAttribute> projections = singletonList(getFieldAttribute("b"));
        EsqlProject keep = new EsqlProject(EMPTY, fa, projections);
        Filter fb = new Filter(EMPTY, keep, conditionB);

        Filter combinedFilter = new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB));
        assertEquals(new EsqlProject(EMPTY, combinedFilter, projections), new PushDownAndCombineFilters().apply(fb));
    }

    // from ... | where a > 1 | stats count(1) by b | where count(1) >= 3 and b < 2
    // => ... | where a > 1 and b < 2 | stats count(1) by b | where count(1) >= 3
    public void testSelectivelyPushDownFilterPastFunctionAgg() {
        EsRelation relation = relation();
        GreaterThan conditionA = greaterThanOf(getFieldAttribute("a"), ONE);
        LessThan conditionB = lessThanOf(getFieldAttribute("b"), TWO);
        GreaterThanOrEqual aggregateCondition = greaterThanOrEqualOf(new Count(EMPTY, ONE), THREE);

        Filter fa = new Filter(EMPTY, relation, conditionA);
        // invalid aggregate but that's fine cause its properties are not used by this rule
        Aggregate aggregate = new Aggregate(
            EMPTY,
            fa,
            Aggregate.AggregateType.STANDARD,
            singletonList(getFieldAttribute("b")),
            emptyList()
        );
        Filter fb = new Filter(EMPTY, aggregate, new And(EMPTY, aggregateCondition, conditionB));

        // expected
        Filter expected = new Filter(
            EMPTY,
            new Aggregate(
                EMPTY,
                new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)),
                Aggregate.AggregateType.STANDARD,
                singletonList(getFieldAttribute("b")),
                emptyList()
            ),
            aggregateCondition
        );
        assertEquals(expected, new PushDownAndCombineFilters().apply(fb));
    }

    private static EsRelation relation() {
        return relation(List.of());
    }

    private static EsRelation relation(List<Attribute> fieldAttributes) {
        return new EsRelation(
            EMPTY,
            new EsIndex(randomAlphaOfLength(8), emptyMap()),
            fieldAttributes,
            randomFrom(IndexMode.values()),
            randomBoolean()
        );
    }
}
