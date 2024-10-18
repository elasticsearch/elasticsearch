/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;

class KqlAstBuilder extends KqlBaseBaseVisitor<QueryBuilder> {
    private final SearchExecutionContext searchExecutionContext;

    KqlAstBuilder(SearchExecutionContext searchExecutionContext) {
        this.searchExecutionContext = searchExecutionContext;
    }

    public QueryBuilder toQueryBuilder(ParserRuleContext ctx) {
        if (ctx instanceof KqlBaseParser.TopLevelQueryContext topLeveQueryContext) {
            if (topLeveQueryContext.query() != null) {
                return ParserUtils.typedParsing(this, topLeveQueryContext.query(), QueryBuilder.class);
            }

            return new MatchAllQueryBuilder();
        }

        throw new IllegalArgumentException("context should be of type TopLevelQueryContext");
    }

    @Override
    public QueryBuilder visitBooleanQuery(KqlBaseParser.BooleanQueryContext ctx) {
        // TODO: implementation
        return new MatchNoneQueryBuilder();
    }

    @Override
    public QueryBuilder visitNotQuery(KqlBaseParser.NotQueryContext ctx) {
        BoolQueryBuilder builder = QueryBuilders.boolQuery();
        return builder.mustNot(ParserUtils.typedParsing(this, ctx.simpleQuery(), QueryBuilder.class));
    }

    @Override
    public QueryBuilder visitExpression(KqlBaseParser.ExpressionContext ctx) {
        // TODO: implementation
        return new MatchNoneQueryBuilder();
    }

    @Override
    public QueryBuilder visitParenthesizedQuery(KqlBaseParser.ParenthesizedQueryContext ctx) {
        return ParserUtils.typedParsing(this, ctx.query(), QueryBuilder.class);
    }

    @Override
    public QueryBuilder visitNestedQuery(KqlBaseParser.NestedQueryContext ctx) {
        // TODO: implementation
        return new MatchNoneQueryBuilder();
    }
}
