/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

// NOCOMMIT
// This Categorize function is a no-op, passing on BytesRefs and rebranding them as categories.
// I doubt that's the way to go, but it gets the job done...

/**
 * Categorizes text messages.
 *
 * This implementation is incomplete and comes with the following caveats:
 * - it only works correctly on a single node.
 * - when running on multiple nodes, category IDs of the different nodes are
 *   aggregated, even though the same ID can correspond to a totally different
 *   category
 * - the output consists of category IDs, which should be replaced by category
 *   regexes or keys
 *
 * TODO(jan, nik): fix this
 */
public class Categorize extends GroupingFunction implements Validatable {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Categorize",
        Categorize::new
    );

    private final Expression field;

    @FunctionInfo(returnType = { "category" }, description = "Categorizes text messages.")
    public Categorize(
        Source source,
        @Param(name = "field", type = { "text", "keyword" }, description = "Expression to categorize") Expression field
    ) {
        super(source, List.of(field));
        this.field = field;
    }

    private Categorize(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    @Evaluator
    static BytesRef process(BytesRef v) {
        return v;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new CategorizeEvaluator.Factory(source(), toEvaluator.apply(field));
    }

    @Override
    protected TypeResolution resolveType() {
        return isString(field(), sourceText(), DEFAULT);
    }

    @Override
    public DataType dataType() {
        return DataType.CATEGORY;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Categorize(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Categorize::new, field);
    }

    public Expression field() {
        return field;
    }

    @Override
    public String toString() {
        return "Categorize{field=" + field + "}";
    }
}
