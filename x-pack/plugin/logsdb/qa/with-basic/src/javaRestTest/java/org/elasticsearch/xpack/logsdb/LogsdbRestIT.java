/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LogsdbRestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testFeatureUsageWithLogsdbIndex() throws IOException {
        {
            var response = getAsMap("/_license/feature_usage");
            @SuppressWarnings("unchecked")
            List<Map<?, ?>> features = (List<Map<?, ?>>) response.get("features");
            assertThat(features, Matchers.empty());
        }
        {
            if (randomBoolean()) {
                createIndex("test-index", Settings.builder().put("index.mode", "logsdb").build());
            } else if (randomBoolean()) {
                String mapping = """
                    {
                        "properties": {
                            "field1": {
                                "type": "keyword",
                                "time_series_dimension": true
                            }
                        }
                    }
                    """;
                var settings = Settings.builder().put("index.mode", "time_series").put("index.routing_path", "field1").build();
                createIndex("test-index", settings, mapping);
            } else {
                String mapping = """
                    {
                        "_source": {
                            "mode": "synthetic"
                        }
                    }
                    """;
                createIndex("test-index", Settings.EMPTY, mapping);
            }
            var response = getAsMap("/_license/feature_usage");
            @SuppressWarnings("unchecked")
            List<Map<?, ?>> features = (List<Map<?, ?>>) response.get("features");
            assertThat(features, Matchers.empty());
        }
    }

}
