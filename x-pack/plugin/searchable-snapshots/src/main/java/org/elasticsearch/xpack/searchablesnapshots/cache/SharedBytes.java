/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.xpack.searchablesnapshots.preallocate.Preallocate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class SharedBytes extends AbstractRefCounted {

    private static final Logger logger = LogManager.getLogger(SharedBytes.class);

    private static final String CACHE_FILE_NAME = "shared_snapshot_cache";

    private static final StandardOpenOption[] OPEN_OPTIONS = new StandardOpenOption[] {
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE };

    final SharedCacheConfiguration sharedCacheConfiguration;

    // TODO: for systems like Windows without true p-write/read support we should split this up into multiple channels since positional
    // operations in #IO are not contention-free there (https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6265734)
    private final FileChannel fileChannel;

    private final Path path;

    SharedBytes(SharedCacheConfiguration sharedCacheConfiguration, NodeEnvironment environment) throws IOException {
        super("shared-bytes");
        this.sharedCacheConfiguration = sharedCacheConfiguration;
        final long fileSize = sharedCacheConfiguration.totalSize();
        Path cacheFile = null;
        if (fileSize > 0) {
            cacheFile = findCacheSnapshotCacheFilePath(environment, fileSize);
            if (cacheFile == null) {
                throw new IOException("Could not find a directory with adequate free space for cache file");
            }
            Preallocate.preallocate(cacheFile, fileSize);
            // TODO: maybe make this faster by allocating a larger direct buffer if this is too slow for very large files
            // We fill either the full file or the bytes between its current size and the desired size once with zeros to fully allocate
            // the file up front
            final ByteBuffer fillBytes = ByteBuffer.allocate(Channels.WRITE_CHUNK_SIZE);
            this.fileChannel = FileChannel.open(cacheFile, OPEN_OPTIONS);
            long written = fileChannel.size();
            if (fileSize < written) {
                logger.info("creating shared snapshot cache file [size={}, path={}]", fileSize, cacheFile);
            } else if (fileSize == written) {
                logger.debug("reusing existing shared snapshot cache file [size={}, path={}]", fileSize, cacheFile);
            }
            fileChannel.position(written);
            while (written < fileSize) {
                final int toWrite = Math.toIntExact(Math.min(fileSize - written, Channels.WRITE_CHUNK_SIZE));
                fillBytes.position(0).limit(toWrite);
                Channels.writeToChannel(fillBytes, fileChannel);
                written += toWrite;
            }
            if (written > fileChannel.size()) {
                fileChannel.truncate(fileSize);
            }
        } else {
            this.fileChannel = null;
            for (Path path : environment.nodeDataPaths()) {
                Files.deleteIfExists(path.resolve(CACHE_FILE_NAME));
            }
        }
        this.path = cacheFile;
    }

    /**
     * Tries to find a suitable path to a searchable snapshots shared cache file in the data paths founds in the environment.
     *
     * @return path for the cache file or {@code null} if none could be found
     */
    @Nullable
    public static Path findCacheSnapshotCacheFilePath(NodeEnvironment environment, long fileSize) throws IOException {
        Path cacheFile = null;
        for (Path path : environment.nodeDataPaths()) {
            Files.createDirectories(path);
            // TODO: be resilient to this check failing and try next path?
            long usableSpace = Environment.getUsableSpace(path);
            Path p = path.resolve(CACHE_FILE_NAME);
            if (Files.exists(p)) {
                usableSpace += Files.size(p);
            }
            // TODO: leave some margin for error here
            if (usableSpace > fileSize) {
                cacheFile = p;
                break;
            }
        }
        return cacheFile;
    }

    @Override
    protected void closeInternal() {
        try {
            IOUtils.close(fileChannel, path == null ? null : () -> Files.deleteIfExists(path));
        } catch (IOException e) {
            logger.warn("Failed to clean up shared bytes file", e);
        }
    }

    private final Map<Integer, IO> ios = ConcurrentCollections.newConcurrentMap();

    IO getFileChannel(int sharedBytesPos) {
        assert fileChannel != null;
        return ios.compute(sharedBytesPos, (p, io) -> {
            if (io == null || io.tryIncRef() == false) {
                final IO newIO;
                boolean success = false;
                incRef();
                try {
                    newIO = new IO(p);
                    success = true;
                } finally {
                    if (success == false) {
                        decRef();
                    }
                }
                return newIO;
            }
            return io;
        });
    }

    long getPhysicalOffset(long sharedPageIndex) {
        return sharedCacheConfiguration.getPhysicalOffset(sharedPageIndex);
    }

    public final class IO extends AbstractRefCounted {

        private final int pageIndex;
        private final long pageStart;

        private IO(final int pageIndex) {
            super("shared-bytes-io");
            this.pageIndex = pageIndex;
            pageStart = getPhysicalOffset(pageIndex);
        }

        @SuppressForbidden(reason = "Use positional reads on purpose")
        public int read(ByteBuffer dst, long position) throws IOException {
            assert refCount() > 0;
            checkOffsets(position, dst.remaining());
            return fileChannel.read(dst, pageStart + position);
        }

        /**
         * Writes the contents of the given buffer to the requested position. This method assumes that both the size of the buffer as well
         * as the write position are 4k aligned.
         *
         * @param src      byte buffer to write (must have a remaining number of bytes that is a multiple of
         *                 {@link SharedCacheConfiguration#TINY_REGION_SIZE} and fits into this region fully
         * @param position position relative to the start index of this instance to write to and that is a multiple of
         *                 {@link SharedCacheConfiguration#TINY_REGION_SIZE}
         */
        public void writeAligned(ByteBuffer src, long position) throws IOException {
            assert src.remaining() % SharedCacheConfiguration.TINY_REGION_SIZE == 0;
            assert position % SharedCacheConfiguration.TINY_REGION_SIZE == 0;
            write(src, position);
        }

        /**
         * Same as {@link #writeAligned} but without alignment assumptions on buffer or write position.
         */
        @SuppressForbidden(reason = "Use positional writes on purpose")
        public void write(ByteBuffer src, long position) throws IOException {
            assert refCount() > 0;
            checkOffsets(position, src.remaining());
            fileChannel.write(src, pageStart + position);
        }

        /**
         * Returns the maximum size of this cache channel's region.
         */
        public long size() {
            assert refCount() > 0;
            return sharedCacheConfiguration.regionSizeBySharedPageIndex(pageIndex);
        }

        private void checkOffsets(long position, long length) {
            final long regionSize = sharedCacheConfiguration.regionSizeBySharedPageIndex(pageIndex);
            if (position < 0 || position + length > regionSize) {
                assert false;
                throw new IllegalArgumentException("bad access");
            }
        }

        @Override
        protected void closeInternal() {
            ios.remove(pageIndex, this);
            SharedBytes.this.decRef();
        }
    }
}
