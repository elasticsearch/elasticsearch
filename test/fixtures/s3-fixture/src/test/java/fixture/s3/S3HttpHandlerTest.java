/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package fixture.s3;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;

public class S3HttpHandlerTest extends ESTestCase {

    public void testSimpleExample() throws IOException {
        final var handler = new S3HttpHandler("bucket", "path");

        assertEquals(
            RestStatus.INTERNAL_SERVER_ERROR,
            handleRequest(handler, randomFrom("GET", "PUT", "POST", "DELETE", "HEAD"), "/not-in-bucket").status()
        );

        assertEquals(RestStatus.NOT_FOUND, handleRequest(handler, "GET", "/bucket/path/blah").status());

        assertEquals(RestStatus.OK, handleRequest(handler, "PUT", "/bucket/path/blah", new BytesArray(new byte[] { 0x0a })).status());

        assertEquals(
            new TestHttpResponse(RestStatus.OK, new BytesArray(new byte[] { 0x0a })),
            handleRequest(handler, "GET", "/bucket/path/blah")
        );
    }

    private record TestHttpResponse(RestStatus status, BytesReference body) {}

    private static TestHttpResponse handleRequest(S3HttpHandler handler, String method, String uri, BytesReference requestBody)
        throws IOException {
        final var httpExchange = new TestHttpExchange(method, uri, requestBody);
        handler.handle(httpExchange);
        assertNotEquals(0, httpExchange.getResponseCode());
        return new TestHttpResponse(RestStatus.fromCode(httpExchange.getResponseCode()), httpExchange.getResponseBodyContents());
    }

    private static TestHttpResponse handleRequest(S3HttpHandler handler, String method, String uri) throws IOException {
        return handleRequest(handler, method, uri, BytesArray.EMPTY);
    }

    private static class TestHttpExchange extends HttpExchange {

        private static final Headers EMPTY_HEADERS = new Headers();

        private final String method;
        private final URI uri;
        private final BytesReference requestBody;

        private final Headers responseHeaders = new Headers();
        private final BytesStreamOutput responseBody = new BytesStreamOutput();
        private int responseCode;

        TestHttpExchange(String method, String uri, BytesReference requestBody) {
            this.method = method;
            this.uri = URI.create(uri);
            this.requestBody = requestBody;
        }

        @Override
        public Headers getRequestHeaders() {
            return EMPTY_HEADERS;
        }

        @Override
        public Headers getResponseHeaders() {
            return responseHeaders;
        }

        @Override
        public URI getRequestURI() {
            return uri;
        }

        @Override
        public String getRequestMethod() {
            return method;
        }

        @Override
        public HttpContext getHttpContext() {
            return null;
        }

        @Override
        public void close() {}

        @Override
        public InputStream getRequestBody() {
            try {
                return requestBody.streamInput();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public OutputStream getResponseBody() {
            return responseBody;
        }

        @Override
        public void sendResponseHeaders(int rCode, long responseLength) {
            this.responseCode = rCode;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public int getResponseCode() {
            return responseCode;
        }

        public BytesReference getResponseBodyContents() {
            return responseBody.bytes();
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public String getProtocol() {
            return "HTTP/1.1";
        }

        @Override
        public Object getAttribute(String name) {
            return null;
        }

        @Override
        public void setAttribute(String name, Object value) {
            fail("setAttribute not implemented");
        }

        @Override
        public void setStreams(InputStream i, OutputStream o) {
            fail("setStreams not implemented");
        }

        @Override
        public HttpPrincipal getPrincipal() {
            fail("getPrincipal not implemented");
            throw new UnsupportedOperationException("getPrincipal not implemented");
        }
    }

}
