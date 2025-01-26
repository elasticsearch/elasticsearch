/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BitUtil;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SyntheticSourceNativeArrayIntegrationTests extends ESSingleNodeTestCase {

    public void testSynthesizeArray() throws Exception {
        var arrayValues = new Object[][] {
            new Object[] { "z", "y", null, "x", null, "v" },
            new Object[] { null, "b", null, "a" },
            new Object[] { null },
            new Object[] { null, null, null },
            new Object[] { "c", "b", "a" } };
        verifySyntheticArray(arrayValues);
    }

    public void testSynthesizeEmptyArray() throws Exception {
        var arrayValues = new Object[][] { new Object[] {} };
        verifySyntheticArray(arrayValues);
    }

    public void testSynthesizeArrayRandom() throws Exception {
        var arrayValues = new Object[][] { generateRandomStringArray(64, 8, false, true) };
        verifySyntheticArray(arrayValues);
    }

    private void verifySyntheticArray(Object[][] arrays) throws IOException {
        var indexService = createIndex(
            "test-index",
            Settings.builder().put("index.mapping.source.mode", "synthetic").put("index.mapping.synthetic_source_keep", "arrays").build(),
            jsonBuilder().startObject()
                .startObject("properties")
                .startObject("field")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
        );
        for (int i = 0; i < arrays.length; i++) {
            var array = arrays[i];

            var indexRequest = new IndexRequest("test-index");
            indexRequest.id("my-id-" + i);
            var source = jsonBuilder().startObject();
            if (array != null) {
                source.startArray("field");
                for (Object arrayValue : array) {
                    source.value(arrayValue);
                }
                source.endArray();
            } else {
                source.field("field").nullValue();
            }
            indexRequest.source(source.endObject());
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client().index(indexRequest).actionGet();

            var searchRequest = new SearchRequest("test-index");
            searchRequest.source().query(new IdsQueryBuilder().addIds("my-id-" + i));
            var searchResponse = client().search(searchRequest).actionGet();
            try {
                var hit = searchResponse.getHits().getHits()[0];
                assertThat(hit.getId(), equalTo("my-id-" + i));
                var sourceAsMap = hit.getSourceAsMap();
                assertThat(sourceAsMap, hasKey("field"));
                var actualArray = (List<?>) sourceAsMap.get("field");
                if (array == null) {
                    assertThat(actualArray, nullValue());
                } else if (array.length == 0) {
                    assertThat(actualArray, empty());
                } else {
                    assertThat(actualArray, Matchers.contains(array));
                }
            } finally {
                searchResponse.decRef();
            }
        }

        indexService.getShard(0).forceMerge(new ForceMergeRequest("test-index").maxNumSegments(1));
        try (var searcher = indexService.getShard(0).acquireSearcher(getTestName())) {
            var reader = searcher.getDirectoryReader();
            for (int i = 0; i < arrays.length; i++) {
                var document = reader.storedFields().document(i);
                // Verify that there is no ignored source:
                List<String> storedFieldNames = document.getFields().stream().map(IndexableField::name).toList();
                assertThat(storedFieldNames, contains("_id", "_recovery_source"));

                // Verify that there is an offset field:
                var binaryDocValues = reader.leaves().get(0).reader().getBinaryDocValues("field.offsets");
                boolean match = binaryDocValues.advanceExact(i);
                if (arrays[i] == null) {
                    assertThat(match, equalTo(false));
                } else {
                    assertThat(match, equalTo(true));
                    var ref = binaryDocValues.binaryValue();
                    try (ByteArrayStreamInput scratch = new ByteArrayStreamInput()) {
                        scratch.reset(ref.bytes, ref.offset, ref.length);
                        int[] offsets = new int[BitUtil.zigZagDecode(scratch.readVInt())];
                        for (int j = 0; j < offsets.length; j++) {
                            offsets[j] = BitUtil.zigZagDecode(scratch.readVInt());
                        }
                        assertThat(offsets, notNullValue());
                    }
                }
            }
        }
    }

}
