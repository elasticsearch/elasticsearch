/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * Generates a base64-encoded, k-ordered UUID string optimized for compression and efficient indexing.
 * <p>
 * This method produces a time-based UUID where slowly changing components like the timestamp appear first,
 * improving prefix-sharing and compression during indexing. It ensures uniqueness across nodes by incorporating
 * a timestamp, a MAC address, and a sequence ID.
 * <p>
 * <b>Timestamp:</b> Represents the current time in milliseconds, ensuring ordering and uniqueness.
 * <br>
 * <b>MAC Address:</b> Ensures uniqueness across different coordinators.
 * <br>
 * <b>Sequence ID:</b> Differentiates UUIDs generated within the same millisecond, ensuring uniqueness even at high throughput.
 * <p>
 * The result is a compact base64-encoded string, optimized for efficient compression of the _id field in an inverted index.
 */
public class TimeBasedKOrderedUUIDGenerator extends TimeBasedUUIDGenerator {
    private static final Base64.Encoder BASE_64_NO_PADDING = Base64.getEncoder().withoutPadding();

    @Override
    public String getBase64UUID() {
        final int sequenceId = this.sequenceNumber.incrementAndGet() & 0x00FF_FFFF;

        // Calculate timestamp to ensure ordering and avoid backward movement in case of time shifts.
        // Uses AtomicLong to guarantee that timestamp increases even if the system clock moves backward.
        // If the sequenceId overflows (reaches 0 within the same millisecond), the timestamp is incremented
        // to ensure strict ordering.
        long timestamp = this.lastTimestamp.accumulateAndGet(
            currentTimeMillis(),
            sequenceId == 0 ? (lastTimestamp, currentTimeMillis) -> Math.max(lastTimestamp, currentTimeMillis) + 1 : Math::max
        );

        final byte[] uuidBytes = new byte[15];
        final ByteBuffer buffer = ByteBuffer.wrap(uuidBytes);

        buffer.put((byte) (timestamp >>> 40)); // changes every 35 years
        buffer.put((byte) (timestamp >>> 32)); // changes every ~50 days
        buffer.put((byte) (timestamp >>> 24)); // changes every ~4.5h
        buffer.put((byte) (timestamp >>> 16)); // changes every ~65 secs

        // MAC address of the coordinator might change if there are many coordinators in the cluster
        // and the indexing api does not necessarily target the same coordinator.
        byte[] macAddress = macAddress();
        assert macAddress.length == 6;
        buffer.put(macAddress, 0, macAddress.length);

        buffer.put((byte) (sequenceId >>> 16));

        // From hereinafter everything is almost like random and does not compress well
        // due to unlikely prefix-sharing
        buffer.put((byte) (timestamp >>> 8));
        buffer.put((byte) (sequenceId >>> 8));
        buffer.put((byte) timestamp);
        buffer.put((byte) sequenceId);

        assert buffer.position() == uuidBytes.length;

        return BASE_64_NO_PADDING.encodeToString(uuidBytes);
    }
}
