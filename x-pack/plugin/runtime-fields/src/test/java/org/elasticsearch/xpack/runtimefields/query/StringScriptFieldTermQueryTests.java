/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.script.Script;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class StringScriptFieldTermQueryTests extends AbstractStringScriptFieldQueryTestCase<StringScriptFieldTermQuery> {
    @Override
    protected StringScriptFieldTermQuery createTestInstance() {
        return new StringScriptFieldTermQuery(randomScript(), leafFactory, randomAlphaOfLength(5), randomAlphaOfLength(6));
    }

    @Override
    protected StringScriptFieldTermQuery copy(StringScriptFieldTermQuery orig) {
        return new StringScriptFieldTermQuery(orig.script(), leafFactory, orig.fieldName(), orig.term());
    }

    @Override
    protected StringScriptFieldTermQuery mutate(StringScriptFieldTermQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        String term = orig.term();
        switch (randomInt(2)) {
            case 0:
                script = randomValueOtherThan(script, this::randomScript);
                break;
            case 1:
                fieldName += "modified";
                break;
            case 2:
                term += "modified";
                break;
            default:
                fail();
        }
        return new StringScriptFieldTermQuery(script, leafFactory, fieldName, term);
    }

    @Override
    public void testMatches() {
        StringScriptFieldTermQuery query = new StringScriptFieldTermQuery(randomScript(), leafFactory, "test", "foo");
        assertTrue(query.matches(List.of("foo")));
        assertFalse(query.matches(List.of("bar")));
        assertTrue(query.matches(List.of("foo", "bar")));
    }

    @Override
    protected void assertToString(StringScriptFieldTermQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo(query.term()));
    }

    @Override
    public void testVisit() {
        StringScriptFieldTermQuery query = createTestInstance();
        Set<Term> allTerms = new TreeSet<>();
        query.visit(new QueryVisitor() {
            @Override
            public void consumeTerms(Query query, Term... terms) {
                allTerms.addAll(Arrays.asList(terms));
            }

            @Override
            public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
                fail();
            }
        });
        assertThat(allTerms, equalTo(Set.of(new Term(query.fieldName(), query.term()))));
    }
}
