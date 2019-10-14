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

package org.elasticsearch.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.elasticsearch.common.Booleans;

public class NettyAllocator {

    public static final ByteBufAllocator ALLOCATOR;

    static {
        if (Booleans.parseBoolean(System.getProperty("es.unsafe.use_netty_default_allocator"), false)) {
            ALLOCATOR = ByteBufAllocator.DEFAULT;
        } else {
            ByteBufAllocator delegate;
            if ("unpooled".equals(System.getProperty("io.netty.allocator.type"))) {
                delegate = UnpooledByteBufAllocator.DEFAULT;
            } else {
                int nHeapArena = PooledByteBufAllocator.defaultNumHeapArena();
                int pageSize = PooledByteBufAllocator.defaultPageSize();
                int maxOrder = PooledByteBufAllocator.defaultMaxOrder();
                int tinyCacheSize = PooledByteBufAllocator.defaultTinyCacheSize();
                int smallCacheSize = PooledByteBufAllocator.defaultSmallCacheSize();
                int normalCacheSize = PooledByteBufAllocator.defaultNormalCacheSize();
                boolean useCacheForAllThreads = PooledByteBufAllocator.defaultUseCacheForAllThreads();
                delegate = new PooledByteBufAllocator(false, nHeapArena, 0, pageSize, maxOrder, tinyCacheSize,
                    smallCacheSize, normalCacheSize, useCacheForAllThreads);
            }
            ALLOCATOR = new NoDirectBuffers(delegate);
        }
    }

    private static class NoDirectBuffers implements ByteBufAllocator {

        private final ByteBufAllocator delegate;

        private NoDirectBuffers(ByteBufAllocator delegate) {
            this.delegate = delegate;
        }

        @Override
        public ByteBuf buffer() {
            return heapBuffer();
        }

        @Override
        public ByteBuf buffer(int initialCapacity) {
            return heapBuffer(initialCapacity);
        }

        @Override
        public ByteBuf buffer(int initialCapacity, int maxCapacity) {
            return heapBuffer(initialCapacity, maxCapacity);
        }

        @Override
        public ByteBuf ioBuffer() {
            return heapBuffer();
        }

        @Override
        public ByteBuf ioBuffer(int initialCapacity) {
            return heapBuffer(initialCapacity);
        }

        @Override
        public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
            return heapBuffer(initialCapacity, maxCapacity);
        }

        @Override
        public ByteBuf heapBuffer() {
            return delegate.heapBuffer();
        }

        @Override
        public ByteBuf heapBuffer(int initialCapacity) {
            return delegate.heapBuffer(initialCapacity);
        }

        @Override
        public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
            return delegate.heapBuffer(initialCapacity, maxCapacity);
        }

        @Override
        public ByteBuf directBuffer() {
            throw new UnsupportedOperationException("Direct buffers not supported.");
        }

        @Override
        public ByteBuf directBuffer(int initialCapacity) {
            throw new UnsupportedOperationException("Direct buffers not supported.");
        }

        @Override
        public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
            throw new UnsupportedOperationException("Direct buffers not supported.");
        }

        @Override
        public CompositeByteBuf compositeBuffer() {
            return compositeHeapBuffer();
        }

        @Override
        public CompositeByteBuf compositeBuffer(int maxNumComponents) {
            return compositeHeapBuffer(maxNumComponents);
        }

        @Override
        public CompositeByteBuf compositeHeapBuffer() {
            return delegate.compositeHeapBuffer();
        }

        @Override
        public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
            return delegate.compositeHeapBuffer(maxNumComponents);
        }

        @Override
        public CompositeByteBuf compositeDirectBuffer() {
            throw new UnsupportedOperationException("Direct buffers not supported.");
        }

        @Override
        public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
            throw new UnsupportedOperationException("Direct buffers not supported.");
        }

        @Override
        public boolean isDirectBufferPooled() {
            return delegate.isDirectBufferPooled();
        }

        @Override
        public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
            return delegate.calculateNewCapacity(minNewCapacity, maxCapacity);
        }
    }
}
