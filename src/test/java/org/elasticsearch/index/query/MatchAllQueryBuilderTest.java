/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import java.io.IOException;

import static org.hamcrest.Matchers.*;

public class MatchAllQueryBuilderTest extends BaseQueryTestCase<MatchAllQueryBuilder> {

    @Override
    protected void assertLuceneQuery(MatchAllQueryBuilder queryBuilder, Query query) throws IOException {
        if (queryBuilder.boost() != 1.0f) {
            assertThat(query, instanceOf(MatchAllDocsQuery.class));
        } else {
            assertThat(query, instanceOf(ConstantScoreQuery.class));
        }
        assertThat(query.getBoost(), is(queryBuilder.boost()));
    }

    @Override
    protected MatchAllQueryBuilder createEmptyQueryBuilder() {
        return new MatchAllQueryBuilder();
    }

    /**
     * @return a MatchAllQuery with random boost between 0.1f and 2.0f
     */
    @Override
    protected MatchAllQueryBuilder createTestQueryBuilder() {
        MatchAllQueryBuilder query = new MatchAllQueryBuilder();
        if (randomBoolean()) {
            query.boost(2.0f / randomIntBetween(1, 20));
        }
        return query;
    }

}