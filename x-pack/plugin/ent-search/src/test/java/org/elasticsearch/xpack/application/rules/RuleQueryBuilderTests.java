/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.application.EnterpriseSearch;
import org.elasticsearch.xpack.application.search.SearchApplicationTestUtils;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.rules.RuleQueryBuilder.ALLOWED_MATCH_CRITERIA;
import static org.hamcrest.CoreMatchers.instanceOf;

public class RuleQueryBuilderTests extends AbstractQueryTestCase<RuleQueryBuilder> {

    protected static String rulesetId;

    private Map<String, Object> generateRandomMatchCriteria() {
        final int randomIndex = Randomness.get().nextInt(ALLOWED_MATCH_CRITERIA.size());
        final String matchCriteria = (String) ALLOWED_MATCH_CRITERIA.toArray()[randomIndex];

        return generateRandomMatchCriteria(matchCriteria);
    }

    private Map<String, Object> generateRandomMatchCriteria(String key) {
        return Map.of(key, randomAlphaOfLengthBetween(1, 10));
    }

    private List<String> generateRandomRulesetIds() {
        List<String> rulesetIds = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 2); i++) {
            rulesetIds.add(randomAlphaOfLengthBetween(5, 10));
        }
        return rulesetIds;
    }

    @Override
    protected RuleQueryBuilder doCreateTestQueryBuilder() {
        return new RuleQueryBuilder(new MatchAllQueryBuilder(), generateRandomMatchCriteria(), generateRandomRulesetIds());
    }

    @Override
    protected void doAssertLuceneQuery(RuleQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        // TODO - Figure out what needs to be validated here.
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(EnterpriseSearch.class);
    }

    public void testIllegalArguments() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new RuleQueryBuilder(new MatchAllQueryBuilder(), null, Collections.singletonList("rulesetId"))
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RuleQueryBuilder(new MatchAllQueryBuilder(), generateRandomMatchCriteria(), null)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RuleQueryBuilder(new MatchAllQueryBuilder(), generateRandomMatchCriteria(), Collections.emptyList())
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RuleQueryBuilder(null, generateRandomMatchCriteria(), Collections.singletonList("rulesetId"))
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RuleQueryBuilder(null, Collections.emptyMap(), Collections.singletonList("rulesetId"))
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RuleQueryBuilder(
                new MatchAllQueryBuilder(),
                generateRandomMatchCriteria("invalid_value"),
                Collections.singletonList("rulesetId")
            )
        );
    }

    public void testFromJson() throws IOException {
        String query = """
            {
              "rule_query": {
                "organic": {
                  "term": {
                    "tag": {
                      "value": "search"
                    }
                  }
                },
                "match_criteria": {
                  "query_string": "elastic"
                },
                "ruleset_ids": [ "ruleset1", "ruleset2" ]
              }
            }""";

        RuleQueryBuilder queryBuilder = (RuleQueryBuilder) parseQuery(query);
        checkGeneratedJson(query, queryBuilder);

        assertEquals(query, 2, queryBuilder.rulesetIds().size());
        assertEquals(query, "elastic", queryBuilder.matchCriteria().get("query_string"));
        assertThat(queryBuilder.organicQuery(), instanceOf(TermQueryBuilder.class));
    }

    /**
    * test that unknown query names in the clauses throw an error
    */
    public void testUnknownQueryName() {
        String query = "{\"rule_query\" : {\"organic\" : { \"unknown_query\" : { } } } }";

        ParsingException ex = expectThrows(ParsingException.class, () -> parseQuery(query));
        assertEquals("[1:50] [rule_query] failed to parse field [organic]", ex.getMessage());
    }

    public void testRewrite() throws IOException {
        RuleQueryBuilder ruleQueryBuilder = new RuleQueryBuilder(
            new TermQueryBuilder("foo", 1),
            Map.of("query_string", "bar"),
            Collections.singletonList("baz")
        );
        QueryBuilder rewritten = ruleQueryBuilder.rewrite(createSearchExecutionContext());
        assertThat(rewritten, instanceOf(RuleQueryBuilder.class));
    }

    @Override
    protected GetResponse executeGet(GetRequest getRequest) {
        String rulesetId = getRequest.id();
        List<QueryRule> rules = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            rules.add(SearchApplicationTestUtils.randomQueryRule());
        }
        QueryRuleset queryRuleset = new QueryRuleset(rulesetId, rules);

        assertThat(getRequest.index(), Matchers.equalTo(QueryRulesIndexService.QUERY_RULES_ALIAS_NAME));

        String json;
        try {
            XContentBuilder builder = queryRuleset.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);
            json = Strings.toString(builder);
        } catch (IOException ex) {
            throw new ElasticsearchException("boom", ex);
        }

        return new GetResponse(
            new GetResult(QueryRulesIndexService.QUERY_RULES_ALIAS_NAME, rulesetId, 0, 1, 0L, true, new BytesArray(json), null, null)
        );
    }
}
