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

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.http.netty4.pipelining.HttpPipelinedRequest;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@ChannelHandler.Sharable
class Netty4HttpRequestHandler extends SimpleChannelInboundHandler<Object> {

    private final Netty4HttpServerTransport serverTransport;
    private final boolean httpPipeliningEnabled;
    private final boolean detailedErrorsEnabled;
    private final ThreadContext threadContext;

    Netty4HttpRequestHandler(Netty4HttpServerTransport serverTransport, boolean detailedErrorsEnabled, ThreadContext threadContext) {
        this.serverTransport = serverTransport;
        this.httpPipeliningEnabled = serverTransport.pipelining;
        this.detailedErrorsEnabled = detailedErrorsEnabled;
        this.threadContext = threadContext;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        final FullHttpRequest request;
        final HttpPipelinedRequest pipelinedRequest;
        if (this.httpPipeliningEnabled && msg instanceof HttpPipelinedRequest) {
            pipelinedRequest = (HttpPipelinedRequest) msg;
            request = (FullHttpRequest) pipelinedRequest.last();
        } else {
            pipelinedRequest = null;
            request = (FullHttpRequest) msg;
        }

        final FullHttpRequest copy =
                new DefaultFullHttpRequest(
                        request.protocolVersion(),
                        request.method(),
                        request.uri(),
                        Unpooled.copiedBuffer(request.content()),
                        request.headers(),
                        request.trailingHeaders());

        Exception badRequestCause = null;
        final XContentType xContentType;
        {
            XContentType innerXContentType;
            try {
                innerXContentType = RestRequest.parseContentType(request.headers().getAll("Content-Type"));
            } catch (final IllegalArgumentException e) {
                innerXContentType = null;
                badRequestCause = e;
            }
            xContentType = innerXContentType;
        }

        final Netty4HttpRequest httpRequest;
        {
            Netty4HttpRequest innerHttpRequest;
            try {
                innerHttpRequest = new Netty4HttpRequest(xContentType, serverTransport.xContentRegistry, copy, ctx.channel());
            } catch (final Exception e) {
                if (badRequestCause == null) {
                    badRequestCause = e;
                } else {
                    badRequestCause.addSuppressed(e);
                }
                innerHttpRequest =
                        new Netty4HttpRequest(
                                xContentType,
                                serverTransport.xContentRegistry,
                                Collections.emptyMap(),
                                copy.uri(),
                                copy,
                                ctx.channel());
            }
            httpRequest = innerHttpRequest;
        }

        final Netty4HttpChannel channel;
        {
            Netty4HttpChannel innerChannel;
            try {
                innerChannel = new Netty4HttpChannel(serverTransport, httpRequest, pipelinedRequest, detailedErrorsEnabled, threadContext);
            } catch (final Exception e) {
                if (badRequestCause == null) {
                    badRequestCause = e;
                } else {
                    badRequestCause.addSuppressed(e);
                }
                final Netty4HttpRequest innerRequest =
                        new Netty4HttpRequest(
                                xContentType,
                                serverTransport.xContentRegistry,
                                Collections.emptyMap(),
                                copy.uri(),
                                copy,
                                ctx.channel());
                innerChannel = new Netty4HttpChannel(serverTransport, innerRequest, pipelinedRequest, detailedErrorsEnabled, threadContext);
            }
            channel = innerChannel;
        }

        if (request.decoderResult().isFailure()) {
            serverTransport.dispatchBadRequest(httpRequest, channel, request.decoderResult().cause());
        } else if (badRequestCause != null) {
            serverTransport.dispatchBadRequest(httpRequest, channel, badRequestCause);
        } else {
            serverTransport.dispatchRequest(httpRequest, channel);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Netty4Utils.maybeDie(cause);
        serverTransport.exceptionCaught(ctx, cause);
    }

}
