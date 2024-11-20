/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public abstract class AbstractComparisonSerializationTests<T extends EsqlBinaryComparison> extends AbstractExpressionSerializationTests<T> {
    protected abstract T create(Source source, Expression left, Expression right);

    @Override
    protected final T createTestInstance() {
        return create(randomSource(), randomChild(), randomChild());
    }

    @Override
    protected final T mutateInstance(T instance) throws IOException {
        Expression left = instance.left();
        Expression right = instance.right();
        if (randomBoolean()) {
            left = randomValueOtherThan(instance.left(), AbstractExpressionSerializationTests::randomChild);
        } else {
            right = randomValueOtherThan(instance.right(), AbstractExpressionSerializationTests::randomChild);
        }
        return create(instance.source(), left, right);
    }
}
