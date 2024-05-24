/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.stats;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class GeoIpStatsActionNodeResponseTests extends ESTestCase {

    public void testInputsAreDefensivelyCopied() {
        DiscoveryNode node = DiscoveryNodeUtils.create("id");
        Set<RetrievedDatabaseInfo> databases = new HashSet<>(
            randomList(10, GeoIpStatsActionNodeResponseTests::randomRetrievedDatabaseInfo)
        );
        Set<String> files = new HashSet<>(randomList(10, () -> randomAlphaOfLengthBetween(5, 10)));
        Set<String> configDatabases = new HashSet<>(randomList(10, () -> randomAlphaOfLengthBetween(5, 10)));
        GeoIpStatsAction.NodeResponse nodeResponse = new GeoIpStatsAction.NodeResponse(
            node,
            GeoIpDownloaderStatsSerializingTests.createRandomInstance(),
            randomBoolean() ? null : CacheStatsSerializingTests.createRandomInstance(),
            databases,
            files,
            configDatabases
        );
        assertThat(nodeResponse.getDatabases(), equalTo(databases));
        assertThat(nodeResponse.getFilesInTemp(), equalTo(files));
        assertThat(nodeResponse.getConfigDatabases(), equalTo(configDatabases));
        databases.add(randomRetrievedDatabaseInfo());
        files.add(randomAlphaOfLength(20));
        configDatabases.add(randomAlphaOfLength(20));
        assertThat(nodeResponse.getDatabases(), not(equalTo(databases)));
        assertThat(nodeResponse.getFilesInTemp(), not(equalTo(files)));
        assertThat(nodeResponse.getConfigDatabases(), not(equalTo(configDatabases)));
    }

    private static RetrievedDatabaseInfo randomRetrievedDatabaseInfo() {
        return new RetrievedDatabaseInfo(
            randomAlphaOfLengthBetween(5, 10),
            randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10),
            randomBoolean() ? null : randomLong(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10)
        );
    }
}
