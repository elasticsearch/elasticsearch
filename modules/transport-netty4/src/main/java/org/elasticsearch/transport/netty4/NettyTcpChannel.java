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

package org.elasticsearch.transport.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.TcpChannel;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

public class NettyTcpChannel implements TcpChannel {

    private final Channel channel;
    private final CompletableFuture<TcpChannel> closeContext = new CompletableFuture<>();

    NettyTcpChannel(Channel channel) {
        this.channel = channel;
        this.channel.closeFuture().addListener(f -> {
            if (f.isSuccess()) {
                closeContext.complete(this);
            } else {
                Throwable cause = f.cause();
                if (cause instanceof Error) {
                    Netty4Utils.maybeDie(cause);
                    closeContext.completeExceptionally(cause);
                } else {
                    closeContext.completeExceptionally(cause);
                }
            }
        });
    }

    @Override
    public void close() {
        channel.close();
    }

    @Override
    public void addCloseListener(ActionListener<TcpChannel> listener) {
        closeContext.whenComplete(ActionListener.toBiConsumer(listener));
    }

    @Override
    public void setSoLinger(int value) {
        channel.config().setOption(ChannelOption.SO_LINGER, value);
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<TcpChannel> listener) {
        final ChannelFuture future = channel.writeAndFlush(Netty4Utils.toByteBuf(reference));
        future.addListener(f -> {
            if (f.isSuccess()) {
                listener.onResponse(this);
            } else {
                final Throwable cause = f.cause();
                Netty4Utils.maybeDie(cause);
                assert cause instanceof Exception;
                listener.onFailure((Exception) cause);
            }
        });
    }

    public Channel getLowLevelChannel() {
        return channel;
    }
}
