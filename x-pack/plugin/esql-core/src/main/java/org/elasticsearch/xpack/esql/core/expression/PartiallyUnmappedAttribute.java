/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.PartiallyUnmappedField;

import java.io.IOException;

// FIXME(gal, do-not-merge!) document
public class PartiallyUnmappedAttribute extends Attribute {
    PartiallyUnmappedField field;

    public PartiallyUnmappedAttribute(Source source, String name, PartiallyUnmappedField field) {
        super(source, name, null);
        this.field = field;
    }

    public PartiallyUnmappedField field() {
        return field;
    }

    @Override
    protected Attribute clone(Source source, String name, DataType type, Nullability nullability, NameId id, boolean synthetic) {
        return this;
    }

    @Override
    protected String label() {
        return "u" + name();
    }

    @Override
    public DataType dataType() {
        throw new UnsupportedOperationException("PartiallyUnmappedAttribute doesn't have a data type");
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, UnresolvedAttribute::new, name());
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the node");
    }
}
