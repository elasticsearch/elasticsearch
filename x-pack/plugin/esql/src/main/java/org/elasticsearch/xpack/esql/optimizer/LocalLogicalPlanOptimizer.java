/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.PropagateEmptyRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.rule.ParameterizedRule;
import org.elasticsearch.xpack.ql.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.cleanup;
import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.operators;
import static org.elasticsearch.xpack.ql.optimizer.OptimizerRules.TransformDirection.UP;

public class LocalLogicalPlanOptimizer extends ParameterizedRuleExecutor<LogicalPlan, LocalLogicalOptimizerContext> {

    public LocalLogicalPlanOptimizer(LocalLogicalOptimizerContext localLogicalOptimizerContext) {
        super(localLogicalOptimizerContext);
    }

    @Override
    protected List<Batch<LogicalPlan>> batches() {
        var local = new Batch<>("Local rewrite", new ReplaceTopNWithLimitAndSort(), new ReplaceMissingFieldWithNull());

        var rules = new ArrayList<Batch<LogicalPlan>>();
        rules.add(local);
        // TODO: if the local rules haven't touched the tree, the rest of the rules can be skipped
        rules.addAll(asList(operators(), cleanup()));
        replaceRules(rules);
        return rules;
    }

    private List<Batch<LogicalPlan>> replaceRules(List<Batch<LogicalPlan>> listOfRules) {
        for (Batch<LogicalPlan> batch : listOfRules) {
            var rules = batch.rules();
            for (int i = 0; i < rules.length; i++) {
                if (rules[i] instanceof PropagateEmptyRelation) {
                    rules[i] = new LocalPropagateEmptyRelation();
                }
            }
        }
        return listOfRules;
    }

    public LogicalPlan localOptimize(LogicalPlan plan) {
        return execute(plan);
    }

    /**
     * Break TopN back into Limit + OrderBy to allow the order rules to kick in.
     */
    public static class ReplaceTopNWithLimitAndSort extends OptimizerRules.OptimizerRule<TopN> {
        public ReplaceTopNWithLimitAndSort() {
            super(UP);
        }

        @Override
        protected LogicalPlan rule(TopN plan) {
            return new Limit(plan.source(), plan.limit(), new OrderBy(plan.source(), plan.child(), plan.order()));
        }
    }

    /**
     * Look for any fields used in the plan that are missing locally and replace them with null.
     * This should minimize the plan execution, in the best scenario skipping its execution all together.
     */
    private static class ReplaceMissingFieldWithNull extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

        @Override
        public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext localLogicalOptimizerContext) {
            return plan.transformUp(p -> missingToNull(p, localLogicalOptimizerContext.searchStats()));
        }

        private LogicalPlan missingToNull(LogicalPlan plan, SearchStats stats) {
            if (plan instanceof EsRelation) {
                return plan;
            }

            if (plan instanceof Aggregate a) {
                // don't do anything (for now)
                return a;
            }
            // keep the aliased name
            else if (plan instanceof Project project) {
                var projections = project.projections();
                List<NamedExpression> newProjections = new ArrayList<>(projections.size());
                List<Alias> literals = new ArrayList<>();

                for (NamedExpression projection : projections) {
                    if (projection instanceof FieldAttribute f && stats.exists(f.qualifiedName()) == false) {
                        var alias = new Alias(f.source(), f.name(), null, Literal.of(f, null), f.id());
                        literals.add(alias);
                        newProjections.add(alias.toAttribute());
                    } else {
                        newProjections.add(projection);
                    }
                }
                if (literals.size() > 0) {
                    plan = new Eval(project.source(), project.child(), literals);
                    plan = new Project(project.source(), plan, newProjections);
                } else {
                    plan = project;
                }
            } else {
                plan = plan.transformExpressionsOnlyUp(
                    FieldAttribute.class,
                    f -> stats.exists(f.qualifiedName()) ? f : Literal.of(f, null)
                );
            }

            return plan;
        }
    }

    /**
     * Local aggregation can only produce intermediate state that get wired into the global agg.
     */
    private static class LocalPropagateEmptyRelation extends PropagateEmptyRelation {

        /**
         * Local variant of the aggregation that returns the intermediate value.
         */
        @Override
        protected void aggOutput(NamedExpression agg, AggregateFunction aggFunc, BlockFactory blockFactory, List<Block> blocks) {
            List<Attribute> output = AbstractPhysicalOperationProviders.intermediateAttributes(List.of(agg), List.of());
            for (Attribute o : output) {
                DataType dataType = o.dataType();
                // boolean right now is used for the internal #seen so always return true
                var value = dataType == DataTypes.BOOLEAN ? true
                    // look for count(literal) with literal != null
                    : aggFunc instanceof Count count && (count.foldable() == false || count.fold() != null) ? 0L
                    // otherwise nullify
                    : null;
                var wrapper = BlockUtils.wrapperFor(blockFactory, PlannerUtils.toElementType(dataType), 1);
                wrapper.accept(value);
                blocks.add(wrapper.builder().build());
            }
        }
    }

    abstract static class ParameterizedOptimizerRule<SubPlan extends LogicalPlan, P> extends ParameterizedRule<SubPlan, LogicalPlan, P> {

        public final LogicalPlan apply(LogicalPlan plan, P context) {
            return plan.transformUp(typeToken(), t -> rule(t, context));
        }

        protected abstract LogicalPlan rule(SubPlan plan, P context);
    }
}
