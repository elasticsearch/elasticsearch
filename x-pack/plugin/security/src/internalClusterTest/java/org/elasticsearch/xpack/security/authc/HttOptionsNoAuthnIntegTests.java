/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class HttOptionsNoAuthnIntegTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // need real http
    }

    public void testNoAuthnForResourceOptionsMethod() throws Exception {
        Request request = new Request("OPTIONS", randomFrom("/", "/_cluster/stats", "/some-index", "/index/_stats", "/_stats/flush"));
        // no "Authorization" request header -> request is unauthenticated
        assertThat(request.getOptions().getHeaders().isEmpty(), is(true));
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(response.getHeader("Allow"), notNullValue());
        assertThat(response.getHeader("X-elastic-product"), is("Elasticsearch"));
        assertThat(response.getHeader("content-length"), is("0"));
        // WRONG "Authorization" request header
        request = new Request("OPTIONS", randomFrom("/", "/_cluster/stats", "/some-index", "/index/_stats", "/_stats/flush"));
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(
            "Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME, new SecureString("WRONG"))
        );
        request.setOptions(options);
        response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(response.getHeader("Allow"), notNullValue());
        assertThat(response.getHeader("X-elastic-product"), is("Elasticsearch"));
        assertThat(response.getHeader("content-length"), is("0"));
    }

}
