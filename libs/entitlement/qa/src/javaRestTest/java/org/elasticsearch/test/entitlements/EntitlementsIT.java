/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.entitlements;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class EntitlementsIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .plugin("entitlement-negative-check")
        .plugin("entitlement-positive-check")
        .plugin("entitlement-nonmodular")
        .systemProperty("es.entitlements.enabled", "true")
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testCheckSystemExitThrows() {
        var exception = expectThrows(
            IOException.class,
            () -> { client().performRequest(new Request("GET", "/_entitlement/negative/_check_system_exit")); }
        );
        assertThat(exception.getMessage(), containsString("not_entitled_exception"));
    }

    public void testCheckSystemExitNonModularPluginThrows() {
        var exception = expectThrows(
            IOException.class,
            () -> { client().performRequest(new Request("GET", "/_entitlement/nonmodular/_check_system_exit")); }
        );
        assertThat(
            exception.getMessage(),
            containsString("Missing entitlement: caller " +
                "[class org.elasticsearch.test.entitlements.RestEntitlementsCheckSystemExitAction], module [null], " +
                "type [ExitVMEntitlement]")
        );
    }

    public void testCheckCreateURLClassLoaderWithoutPolicyThrows() {
        var exception = expectThrows(IOException.class, () -> {
            client().performRequest(new Request("GET", "/_entitlement/negative/_check_create_url_classloader"));
        });
        assertThat(exception.getMessage(), containsString("not_entitled_exception"));
    }

    public void testCheckCreateURLClassLoaderWithPolicyPass() throws IOException {
        Response result = client().performRequest(new Request("GET", "/_entitlement/positive/_check_create_url_classloader"));
        assertThat(result.getStatusLine().getStatusCode(), equalTo(200));
    }
}
