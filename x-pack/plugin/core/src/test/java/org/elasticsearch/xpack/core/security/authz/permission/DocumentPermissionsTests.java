/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.join.query.HasChildQueryBuilder;
import org.elasticsearch.join.query.HasParentQueryBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DocumentPermissionsTests extends ESTestCase {

    public void testHasDocumentPermissions() throws IOException {
        final DocumentPermissions documentPermissions1 = DocumentPermissions.allowAll();
        assertThat(documentPermissions1, is(notNull()));
        assertThat(documentPermissions1.hasDocumentLevelPermissions(), is(false));
        assertThat(DocumentPermissions.filter(null, null, null, null, documentPermissions1), is(nullValue()));

        final DocumentPermissions documentPermissions2 = DocumentPermissions
                .filteredBy(Collections.singleton(new BytesArray("{\"match_all\" : {}}")));
        assertThat(documentPermissions2, is(notNull()));
        assertThat(documentPermissions2.hasDocumentLevelPermissions(), is(true));

        final DocumentPermissions documentPermissions3 = DocumentPermissions
                .scopedDocumentPermissions(documentPermissions1, documentPermissions2);
        assertThat(documentPermissions3, is(notNull()));
        assertThat(documentPermissions3.hasDocumentLevelPermissions(), is(true));
    }

    public void testVerifyRoleQuery() throws Exception {
        QueryBuilder queryBuilder1 = new TermsQueryBuilder("field", "val1", "val2");
        DocumentPermissions.verifyRoleQuery(queryBuilder1);

        QueryBuilder queryBuilder2 = new TermsQueryBuilder("field", new TermsLookup("_index", "_type", "_id", "_path"));
        Exception e = expectThrows(IllegalArgumentException.class, () -> DocumentPermissions.verifyRoleQuery(queryBuilder2));
        assertThat(e.getMessage(), equalTo("terms query with terms lookup isn't supported as part of a role query"));

        QueryBuilder queryBuilder3 = new GeoShapeQueryBuilder("field", "_id", "_type");
        e = expectThrows(IllegalArgumentException.class, () -> DocumentPermissions.verifyRoleQuery(queryBuilder3));
        assertThat(e.getMessage(), equalTo("geoshape query referring to indexed shapes isn't support as part of a role query"));

        QueryBuilder queryBuilder4 = new HasChildQueryBuilder("_type", new MatchAllQueryBuilder(), ScoreMode.None);
        e = expectThrows(IllegalArgumentException.class, () -> DocumentPermissions.verifyRoleQuery(queryBuilder4));
        assertThat(e.getMessage(), equalTo("has_child query isn't support as part of a role query"));

        QueryBuilder queryBuilder5 = new HasParentQueryBuilder("_type", new MatchAllQueryBuilder(), false);
        e = expectThrows(IllegalArgumentException.class, () -> DocumentPermissions.verifyRoleQuery(queryBuilder5));
        assertThat(e.getMessage(), equalTo("has_parent query isn't support as part of a role query"));

        QueryBuilder queryBuilder6 = new BoolQueryBuilder().must(new GeoShapeQueryBuilder("field", "_id", "_type"));
        e = expectThrows(IllegalArgumentException.class, () -> DocumentPermissions.verifyRoleQuery(queryBuilder6));
        assertThat(e.getMessage(), equalTo("geoshape query referring to indexed shapes isn't support as part of a role query"));

        QueryBuilder queryBuilder7 = new ConstantScoreQueryBuilder(new GeoShapeQueryBuilder("field", "_id", "_type"));
        e = expectThrows(IllegalArgumentException.class, () -> DocumentPermissions.verifyRoleQuery(queryBuilder7));
        assertThat(e.getMessage(), equalTo("geoshape query referring to indexed shapes isn't support as part of a role query"));

        QueryBuilder queryBuilder8 = new FunctionScoreQueryBuilder(new GeoShapeQueryBuilder("field", "_id", "_type"));
        e = expectThrows(IllegalArgumentException.class, () -> DocumentPermissions.verifyRoleQuery(queryBuilder8));
        assertThat(e.getMessage(), equalTo("geoshape query referring to indexed shapes isn't support as part of a role query"));

        QueryBuilder queryBuilder9 = new BoostingQueryBuilder(new GeoShapeQueryBuilder("field", "_id", "_type"),
                new MatchAllQueryBuilder());
        e = expectThrows(IllegalArgumentException.class, () -> DocumentPermissions.verifyRoleQuery(queryBuilder9));
        assertThat(e.getMessage(), equalTo("geoshape query referring to indexed shapes isn't support as part of a role query"));
    }

    public void testFailIfQueryUsesClient() throws Exception {
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        final long nowInMillis = randomNonNegativeLong();
        QueryRewriteContext context = new QueryRewriteContext(xContentRegistry(), writableRegistry(), client,
                () -> nowInMillis);
        QueryBuilder queryBuilder1 = new TermsQueryBuilder("field", "val1", "val2");
        DocumentPermissions.failIfQueryUsesClient(queryBuilder1, context);

        QueryBuilder queryBuilder2 = new TermsQueryBuilder("field", new TermsLookup("_index", "_type", "_id", "_path"));
        Exception e = expectThrows(IllegalStateException.class,
                () -> DocumentPermissions.failIfQueryUsesClient(queryBuilder2, context));
        assertThat(e.getMessage(), equalTo("role queries are not allowed to execute additional requests"));
    }
}
