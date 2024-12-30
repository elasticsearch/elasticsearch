/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;

import static org.elasticsearch.xpack.esql.planner.PlannerUtils.convertToVerificationException;

public final class ConstantFolding extends OptimizerRules.OptimizerExpressionRule<Expression> {

    public ConstantFolding() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public Expression rule(Expression e) {
        try {
            return e.foldable() ? Literal.of(e) : e;
        } catch (Exception exception) {
            throw convertToVerificationException(exception, e.source().source());
        }
    }
}
