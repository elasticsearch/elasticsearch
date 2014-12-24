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
package org.elasticsearch.percolator;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.percolator.PercolatorException;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertMatchCount;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 */
public class PercolatorBackwardsCompatibilityTests extends ElasticsearchIntegrationTest {

    @Test
    public void testPercolatorUpgrading() throws Exception {
        // Simulates an index created on an node before 1.4.0 where the field resolution isn't strict.
        assertAcked(prepareCreate("test")
                .setSettings(settings(Version.V_1_3_0).put(indexSettings())));
        ensureGreen();
        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", PercolatorService.TYPE_NAME)
                    .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "value")).endObject());
        }
        indexRandom(true, docs);
        PercolateResponse response = client().preparePercolate().setIndices("test").setDocumentType("type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("field1", "value"))
                .get();
        assertMatchCount(response, (long) numDocs);

        // After upgrade indices, indices created before the upgrade allow that queries refer to fields not available in mapping
        client().prepareIndex("test", PercolatorService.TYPE_NAME)
                .setSource(jsonBuilder().startObject().field("query", termQuery("field2", "value")).endObject()).get();

        // However on new indices, the field resolution is strict, no queries with unmapped fields are allowed
        createIndex("test1");
        try {
            client().prepareIndex("test1", PercolatorService.TYPE_NAME)
                    .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "value")).endObject()).get();
            fail();
        } catch (PercolatorException e) {
            e.printStackTrace();
            assertThat(e.getRootCause(), instanceOf(QueryParsingException.class));
        }

        // If and only if index.percolator.map_unmapped_fields_as_string is set to true, unmapped field in query is allowed.
        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder()
                .put(indexSettings())
                .put("index.percolator.map_unmapped_fields_as_string", true);
        assertAcked(prepareCreate("test2")
                .setSettings(settings));
        client().prepareIndex("test2", PercolatorService.TYPE_NAME)
                .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "value")).endObject()).get();
        logger.info("--> Percolate doc with field1=value");
        PercolateResponse response1 = client().preparePercolate()
                .setIndices("test2").setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "value").endObject()))
                .execute().actionGet();
        assertMatchCount(response1, 1l);
        assertThat(response1.getMatches(), arrayWithSize(1));
    }
}
