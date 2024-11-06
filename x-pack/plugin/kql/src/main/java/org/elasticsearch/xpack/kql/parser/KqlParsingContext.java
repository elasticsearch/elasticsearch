/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.AbstractScriptFieldType;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryRewriteContext;

import java.time.ZoneId;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Tuple.tuple;

public class KqlParsingContext {

    private static final List<String> IGNORED_METADATA_FIELDS = List.of(
        "_seq_no",
        "_index_mode",
        "_routing",
        "_ignored",
        "_nested_path",
        "_field_names"
    );

    private static final Predicate<Tuple<String, MappedFieldType>> searchableFieldFilter = (fieldDef) -> fieldDef.v2().isSearchable();
    private static final Predicate<Tuple<String, MappedFieldType>> ignoredFieldFilter = (fieldDef) -> IGNORED_METADATA_FIELDS.contains(
        fieldDef.v1()
    );

    public static Builder builder(QueryRewriteContext queryRewriteContext) {
        return new Builder(queryRewriteContext);
    }

    private QueryRewriteContext queryRewriteContext;
    private final boolean caseInsensitive;
    private final ZoneId timeZone;
    private final String defaultField;

    public KqlParsingContext(QueryRewriteContext queryRewriteContext, boolean caseInsensitive, ZoneId timeZone, String defaultField) {
        this.queryRewriteContext = queryRewriteContext;
        this.caseInsensitive = caseInsensitive;
        this.timeZone = timeZone;
        this.defaultField = defaultField;
    }

    public Iterable<Tuple<String, MappedFieldType>> resolveFields(KqlBaseParser.FieldNameContext fieldNameContext) {
        // TODO: use index settings default field.
        String fieldNamePattern = fieldNameContext != null ? ParserUtils.extractText(fieldNameContext) : "*";
        Set<String> matchingFieldNames = queryRewriteContext.getMatchingFieldNames(fieldNamePattern);

        if (fieldNameContext != null && fieldNameContext.value != null && fieldNameContext.value.getType() == KqlBaseParser.QUOTED_STRING) {
            if (matchingFieldNames.isEmpty() || fieldNamePattern.contains("*")) {
                return List.of();
            }

            return List.of(tuple(fieldNamePattern, queryRewriteContext.getFieldType(fieldNamePattern)));
        }

        return matchingFieldNames.stream()
            .map(fieldName -> tuple(fieldName, queryRewriteContext.getFieldType(fieldName)))
            .filter(searchableFieldFilter.and(Predicate.not(ignoredFieldFilter)))
            .collect(Collectors.toList());
    }

    public boolean caseInsensitive() {
        return caseInsensitive;
    }

    public ZoneId timeZone() {
        return timeZone;
    }

    public static boolean isRuntimeField(MappedFieldType fieldType) {
        return fieldType instanceof AbstractScriptFieldType<?>;
    }

    public static boolean isDateField(MappedFieldType fieldType) {
        return fieldType.typeName().equals(DateFieldMapper.CONTENT_TYPE);
    }

    public static boolean isKeywordField(MappedFieldType fieldType) {
        return fieldType.typeName().equals(KeywordFieldMapper.CONTENT_TYPE);
    }

    public static class Builder {
        private final QueryRewriteContext queryRewriteContext;
        private boolean caseInsensitive = true;
        private ZoneId timeZone = null;
        private String defaultField = null;

        private Builder(QueryRewriteContext queryRewriteContext) {
            this.queryRewriteContext = queryRewriteContext;
        }

        public KqlParsingContext build() {
            return new KqlParsingContext(queryRewriteContext, caseInsensitive, timeZone, defaultField);
        }

        public Builder caseInsensitive(boolean caseInsensitive) {
            this.caseInsensitive = caseInsensitive;
            return this;
        }

        public Builder timeZone(ZoneId timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public Builder defaultField(String defaultField) {
            this.defaultField = defaultField;
            return this;
        }
    }
}
