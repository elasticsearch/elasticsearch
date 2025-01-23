/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.Objects;

public class Rerank extends InferencePlan {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Rerank", Rerank::new);
    private final Expression queryText;
    private final Expression input;

    public Rerank(Source source, LogicalPlan child, Expression queryText, Expression input, Expression inferenceId) {
        super(source, child, inferenceId);
        this.queryText = queryText;
        this.input = input;
    }

    public Rerank(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(queryText);
        out.writeNamedWriteable(input);
        out.writeNamedWriteable(inferenceId());
    }

    public Expression queryText() {
        return queryText;
    }

    public Expression input() {
        return input;
    }

    @Override
    public TaskType taskType() {
        return TaskType.RERANK;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public String commandName() {
        return "RERANK";
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Rerank(source(), newChild, queryText, input, inferenceId());
    }

    @Override
    public boolean expressionsResolved() {
        return queryText.resolved() && input.resolved() && inferenceId().resolved();
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Rerank::new, child(), queryText, input, inferenceId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Rerank rerank = (Rerank) o;
        return Objects.equals(queryText, rerank.queryText) && Objects.equals(input, rerank.input);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), queryText, input);
    }
}
