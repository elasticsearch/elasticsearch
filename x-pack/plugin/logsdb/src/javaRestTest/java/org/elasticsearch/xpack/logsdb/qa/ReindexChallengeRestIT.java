/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public abstract class ReindexChallengeRestIT extends StandardVersusLogsIndexModeRandomDataChallengeRestIT {

    @Override
    public void indexDocuments(
        final CheckedSupplier<List<XContentBuilder>, IOException> baselineSupplier,
        final CheckedSupplier<List<XContentBuilder>, IOException> contencontenderSupplierderSupplier
    ) throws IOException {
        indexBaselineDocuments(baselineSupplier);
        indexContenderDocuments();
    }

    private Map<String, Object> indexBaselineDocuments(final CheckedSupplier<List<XContentBuilder>, IOException> documentsSupplier)
        throws IOException {
        final StringBuilder sb = new StringBuilder();
        int id = 0;
        for (var document : documentsSupplier.get()) {
            sb.append(Strings.format("{ \"create\": { \"_id\" : \"%d\" } }\n", id));
            sb.append(Strings.toString(document)).append("\n");
            id++;
        }
        var request = new Request("POST", "/" + getBaselineDataStreamName() + "/_bulk");
        request.setJsonEntity(sb.toString());
        request.addParameter("refresh", "true");
        return performBulkRequest(request, true);
    }

    private Map<String, Object> indexContenderDocuments() throws IOException {
        var reindexRequest = new Request("POST", "/_reindex?refresh=true");
        reindexRequest.setJsonEntity(String.format(Locale.ROOT, """
            {
                "source": {
                    "index": "%s"
                },
                "dest": {
                  "index": "%s",
                  "op_type": "create"
                }
            }
            """, getBaselineDataStreamName(), getContenderDataStreamName()));
        var response = client.performRequest(reindexRequest);
        assertOK(response);

        var body = entityAsMap(response);
        assertThat("encountered failures when performing reindex:\n " + body, body.get("failures"), equalTo(List.of()));

        return body;
    }
}
