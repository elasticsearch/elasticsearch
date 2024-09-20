/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.capabilities.UnresolvedException;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

// unfortunately we can't use UnresolvedNamedExpression
public class UnresolvedAttribute extends Attribute implements Unresolvable {
    /**
     * First segment if this is a dot-separated name like {@code a.b.c}.
     * Here, a could just be part of the name OR a qualifier, so we keep it around.
     * Escaped dots do not count as qualifier separators, so e.g. for `a.b.c` this is {@code null}.
     */
    private final String qualifier;
    private final String unresolvedMsg;
    private final boolean customMessage;

    public UnresolvedAttribute(Source source, @Nullable String qualifier, String name) {
        this(source, qualifier, name, null);
    }

    public UnresolvedAttribute(Source source, @Nullable String qualifier, String name, String unresolvedMessage) {
        this(source, qualifier, name, null, unresolvedMessage);
    }

    @SuppressWarnings("this-escape")
    public UnresolvedAttribute(Source source, @Nullable String qualifier, String name, NameId id, String unresolvedMessage) {
        super(source, qualifier == null ? name : qualifier + "." + name, id);
        this.qualifier = qualifier;
        this.customMessage = unresolvedMessage != null;
        this.unresolvedMsg = unresolvedMessage == null ? errorMessage(name(), null) : unresolvedMessage;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    @Override
    protected NodeInfo<UnresolvedAttribute> info() {
        return NodeInfo.create(this, UnresolvedAttribute::new, qualifier(), name(), id(), unresolvedMsg);
    }

    public String qualifier() {
        return qualifier;
    }

    public boolean customMessage() {
        return customMessage;
    }

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    protected Attribute clone(Source source, String name, DataType dataType, Nullability nullability, NameId id, boolean synthetic) {
        return this;
    }

    public UnresolvedAttribute withUnresolvedMessage(String unresolvedMessage) {
        return new UnresolvedAttribute(source(), qualifier(), name(), id(), unresolvedMessage);
    }

    @Override
    protected TypeResolution resolveType() {
        return new TypeResolution("unresolved attribute [" + name() + "]");
    }

    @Override
    public DataType dataType() {
        throw new UnresolvedException("dataType", this);
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + name();
    }

    @Override
    protected String label() {
        return UNRESOLVED_PREFIX;
    }

    @Override
    public String nodeString() {
        return toString();
    }

    @Override
    public String unresolvedMessage() {
        return unresolvedMsg;
    }

    public static String errorMessage(String name, List<String> potentialMatches) {
        String msg = "Unknown column [" + name + "]";
        if (CollectionUtils.isEmpty(potentialMatches) == false) {
            msg += ", did you mean "
                + (potentialMatches.size() == 1 ? "[" + potentialMatches.get(0) + "]" : "any of " + potentialMatches.toString())
                + "?";
        }
        return msg;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), unresolvedMsg, qualifier);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            UnresolvedAttribute ua = (UnresolvedAttribute) obj;
            return Objects.equals(unresolvedMsg, ua.unresolvedMsg) && Objects.equals(qualifier, ua.qualifier);
        }
        return false;
    }
}
