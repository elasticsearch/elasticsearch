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

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.http.HttpPipelinedMessage;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.util.List;

public class Netty4HttpResponse extends DefaultFullHttpResponse implements HttpResponse, HttpPipelinedMessage {

    private final int sequence;
    private final Netty4HttpRequest request;
    private final RestStatus status;

    Netty4HttpResponse(Netty4HttpRequest request, RestStatus status, BytesReference content) {
        super(request.nettyRequest().protocolVersion(), HttpResponseStatus.valueOf(status.getStatus()), Netty4Utils.toByteBuf(content));
        this.sequence = request.sequence();
        this.request = request;
        this.status = status;
    }

    @Override
    public RestStatus getRestStatus() {
        return status;
    }

    @Override
    public void addHeader(String name, String value) {
        headers().add(name, value);
    }

    @Override
    public List<String> getAllHeaders(String name) {
        return headers().getAll(name);
    }

    @Override
    public boolean containsHeader(String name) {
        return headers().contains(name);
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    public Netty4HttpRequest getRequest() {
        return request;
    }
}

