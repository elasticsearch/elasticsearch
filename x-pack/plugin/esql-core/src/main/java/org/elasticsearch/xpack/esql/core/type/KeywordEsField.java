/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.util.PlanStreamInput.readCachedStringWithVersionCheck;
import static org.elasticsearch.xpack.esql.core.util.PlanStreamOutput.writeCachedStringWithVersionCheck;

/**
 * Information about a field in an ES index with the {@code keyword} type.
 */
public class KeywordEsField extends EsField {

    private final int precision;
    private final boolean normalized;
    // If the field is unmapped, attempt to read it from source.
    private final boolean readFromSource;

    public KeywordEsField(String name) {
        this(name, Collections.emptyMap(), true, Short.MAX_VALUE, false);
    }

    public KeywordEsField(String name, Map<String, EsField> properties, boolean hasDocValues, int precision, boolean normalized) {
        this(name, properties, hasDocValues, precision, normalized, false);
    }

    public KeywordEsField(
        String name,
        Map<String, EsField> properties,
        boolean hasDocValues,
        int precision,
        boolean normalized,
        boolean isAlias
    ) {
        this(name, KEYWORD, properties, hasDocValues, precision, normalized, isAlias, false);
    }

    protected KeywordEsField(
        String name,
        DataType esDataType,
        Map<String, EsField> properties,
        boolean hasDocValues,
        int precision,
        boolean normalized,
        boolean isAlias,
        boolean readFromSource
    ) {
        super(name, esDataType, properties, hasDocValues, isAlias);
        this.precision = precision;
        this.normalized = normalized;
        this.readFromSource = readFromSource;
    }

    public KeywordEsField(StreamInput in) throws IOException {
        this(
            readCachedStringWithVersionCheck(in),
            KEYWORD,
            in.readImmutableMap(EsField::readFrom),
            in.readBoolean(),
            in.readInt(),
            in.readBoolean(),
            in.readBoolean(),
            in.readBoolean()
        );
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        writeCachedStringWithVersionCheck(out, getName());
        out.writeMap(getProperties(), (o, x) -> x.writeTo(out));
        out.writeBoolean(isAggregatable());
        out.writeInt(precision);
        out.writeBoolean(normalized);
        out.writeBoolean(isAlias());
    }

    public String getWriteableName() {
        return "KeywordEsField";
    }

    public int getPrecision() {
        return precision;
    }

    public boolean getNormalized() {
        return normalized;
    }

    public KeywordEsField withReadFromSource() {
        return new KeywordEsField(getName(), getDataType(), getProperties(), isAggregatable(), precision, normalized, isAlias(), true);
    }

    public boolean getReadFromSource() {
        return readFromSource;
    }

    @Override
    public Exact getExactInfo() {
        return new Exact(normalized == false, "Normalized keyword field cannot be used for exact match operations");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        KeywordEsField that = (KeywordEsField) o;
        return precision == that.precision && normalized == that.normalized;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision, normalized);
    }
}
