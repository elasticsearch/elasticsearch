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

package org.elasticsearch.transport.nio.channel;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.nio.AcceptingSelector;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;

public class NioServerSocketChannel extends AbstractNioChannel<ServerSocketChannel> {

    private final ChannelFactory channelFactory;

    public NioServerSocketChannel(ServerSocketChannel socketChannel, ChannelFactory channelFactory, AcceptingSelector selector)
        throws IOException {
        super(socketChannel, selector);
        this.channelFactory = channelFactory;
    }

    public ChannelFactory getChannelFactory() {
        return channelFactory;
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        throw new UnsupportedOperationException("Cannot send a message to a server channel.");
    }

    @Override
    public String toString() {
        return "NioServerSocketChannel{" +
            "localAddress=" + getLocalAddress() +
            '}';
    }
}
