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

package org.elasticsearch.common.io;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;

public abstract class Channels {
    /**
     * The maximum chunk size for reads in bytes
     */
    private static final int READ_CHUNK_SIZE = 16384;
    /**
     * The maximum chunk size for writes in bytes
     */
    private static final int WRITE_CHUNK_SIZE = 8192;

    /**
     * read <i>length</i> bytes from <i>position</i> of a file channel
     */
    public static byte[] readFromFileChannel(FileChannel channel, long position, int length) throws IOException {
        byte[] res = new byte[length];
        readFromFileChannel(channel, position, res, 0, length);
        return res;

    }

    /**
     * read <i>length</i> bytes from <i>position</i> of a file channel
     *
     * @param channel         channel to read from
     * @param channelPosition position to read from
     * @param dest            destination byte array to put data in
     * @param destOffset      offset in dest to read into
     * @param length          number of bytes to read
     */
    public static void readFromFileChannel(FileChannel channel, long channelPosition, byte[] dest, int destOffset, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(dest, destOffset, length);
        while (length > 0) {
            final int toRead = Math.min(READ_CHUNK_SIZE, length);
            buffer.limit(buffer.position() + toRead);
            assert buffer.remaining() == toRead;
            final int i = channel.read(buffer, channelPosition);
            if (i < 0) { // be defensive here, even though we checked before hand, something could have changed
                throw new EOFException("read past EOF. pos [" + channelPosition + "] chunkLen: [" + toRead + "] end: [" + channel.size() + "]");
            }
            assert i > 0 : "FileChannel.read with non zero-length bb.remaining() must always read at least one byte (FileChannel is in blocking mode, see spec of ReadableByteChannel)";
            channelPosition += i;
            length -= i;
        }
        assert length == 0;
    }

    /**
     * Copies bytes from source {@link org.jboss.netty.buffer.ChannelBuffer} to a {@link java.nio.channels.GatheringByteChannel}
     *
     * @param source      ChannelBuffer to copy from
     * @param sourceIndex index in <i>source</i> to start copying from
     * @param length      how many bytes to copy
     * @param channel     target GatheringByteChannel
     * @throws IOException
     */
    public static void writeToChannel(ChannelBuffer source, int sourceIndex, int length, GatheringByteChannel channel) throws IOException {
        while (length > 0) {
            int written = source.getBytes(sourceIndex, channel, length);
            sourceIndex += written;
            length -= written;
        }
        assert length == 0;
    }

    /**
     * Writes part of a byte array to a {@link java.nio.channels.WritableByteChannel}
     *
     * @param source  byte array to copy from
     * @param offset  start copying from this offset
     * @param length  how many bytes to copy
     * @param channel target WritableByteChannel
     * @throws IOException
     */
    public static void writeToChannel(byte[] source, int offset, int length, WritableByteChannel channel) throws IOException {
        int toWrite = Math.min(length, WRITE_CHUNK_SIZE);
        ByteBuffer buffer = ByteBuffer.wrap(source, offset, toWrite);
        int written = channel.write(buffer);
        length -= written;
        while (length > 0) {
            toWrite = Math.min(length, WRITE_CHUNK_SIZE);
            buffer.limit(buffer.position() + toWrite);
            written = channel.write(buffer);
            length -= written;
        }
        assert length == 0;
    }

    /**
     * Writes a {@link java.nio.ByteBuffer} to a {@link java.nio.channels.WritableByteChannel}
     *
     * @param byteBuffer source buffer
     * @param channel    channel to write to
     * @throws IOException
     */
    public static void writeToChannel(ByteBuffer byteBuffer, WritableByteChannel channel) throws IOException {
        do {
            channel.write(byteBuffer);
        }
        while (byteBuffer.position() != byteBuffer.limit());
    }
}
