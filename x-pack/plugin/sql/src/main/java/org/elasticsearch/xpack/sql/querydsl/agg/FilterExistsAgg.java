/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.xpack.ql.expression.gen.script.Scripts.formatTemplate;

/**
 * Aggregation builder for a "filter" aggregation encapsulating an "exists" query.
 */
public class FilterExistsAgg extends LeafAgg {

    public FilterExistsAgg(String id, AggSource source) {
        super(id, source);
    }

    @Override
    AggregationBuilder toBuilder() {
        if (source().fieldName() != null) {
            return filter(id(), QueryBuilders.existsQuery(source().fieldName()));
        } else {
            return filter(id(), QueryBuilders.scriptQuery(wrapWithIsNotNull(source().script()).toPainless()));
        }
    }

    private static ScriptTemplate wrapWithIsNotNull(ScriptTemplate script) {
        return new ScriptTemplate(formatTemplate(
                format(Locale.ROOT, "{ql}.isNotNull(%s)", script.template())),
                script.params(),
                DataTypes.BOOLEAN);
    }
}
