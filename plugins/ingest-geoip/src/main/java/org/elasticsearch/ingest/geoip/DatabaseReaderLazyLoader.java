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

package org.elasticsearch.ingest.geoip;

import com.maxmind.geoip2.DatabaseReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Facilitates lazy loading of the database reader, so that when the geoip plugin is installed, but not used,
 * no memory is being wasted on the database reader.
 */
class DatabaseReaderLazyLoader implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(DatabaseReaderLazyLoader.class);

    private final Path databasePath;
    private final CheckedSupplier<DatabaseReader, IOException> loader;
    final SetOnce<DatabaseReader> databaseReader;

    DatabaseReaderLazyLoader(final Path databasePath, final CheckedSupplier<DatabaseReader, IOException> loader) {
        this.databasePath = Objects.requireNonNull(databasePath);
        this.loader = Objects.requireNonNull(loader);
        this.databaseReader = new SetOnce<>();
    }

    final String getDatabaseType() throws IOException {
        final long fileSize = Files.size(databasePath);
        final int[] DATABASE_TYPE_MARKER = {'d', 'a', 't', 'a', 'b', 'a', 's', 'e', '_', 't', 'y', 'p', 'e'};
        try (InputStream in = databaseInputStream()) {
            // read last 512 bytes
            in.skip(fileSize - 512);
            byte[] tail = new byte[512];
            in.read(tail);

            // find the database_type header
            int metadataOffset = -1;
            int markerOffset = 0;
            for (int i = 0; i < tail.length; i++) {
                byte b = tail[i];

                if (b == DATABASE_TYPE_MARKER[markerOffset]) {
                    markerOffset++;
                } else {
                    markerOffset = 0;
                }
                if (markerOffset == DATABASE_TYPE_MARKER.length) {
                    metadataOffset = i + 1;
                    break;
                }
            }

            // read the database type
            final int offsetByte = tail[metadataOffset] & 0xFF;
            final int type = offsetByte >>> 5;
            if (type != 2) {
                throw new RuntimeException("type must be UTF8_STRING");
            }
            int size = offsetByte & 0x1f;
            return new String(tail, metadataOffset + 1, size, StandardCharsets.UTF_8);
        }
    }

    InputStream databaseInputStream() throws IOException {
        return Files.newInputStream(databasePath);
    }

    synchronized DatabaseReader get() throws IOException {
        if (databaseReader.get() == null) {
            synchronized (databaseReader) {
                if (databaseReader.get() == null) {
                    databaseReader.set(loader.get());
                    LOGGER.debug("Loaded [{}] geo-IP database", databasePath);
                }
            }
        }
        return databaseReader.get();
    }

    @Override
    public synchronized void close() throws IOException {
        IOUtils.close(databaseReader.get());
    }

}
