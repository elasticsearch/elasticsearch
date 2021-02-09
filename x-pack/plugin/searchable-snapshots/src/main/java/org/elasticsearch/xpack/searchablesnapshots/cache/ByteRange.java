/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.common.Nullable;

public final class ByteRange implements Comparable<ByteRange> {

    public static final ByteRange EMPTY = new ByteRange(0L, 0L);

    private final long start;

    private final long end;

    public static ByteRange of(long start, long end) {
        return new ByteRange(start, end);
    }

    private ByteRange(long start, long end) {
        this.start = start;
        this.end = end;
        assert start >= 0L : "Start must be >= 0 but saw [" + start + "]";
        assert end >= start : "End must be greater or equal to start but saw [" + start + "][" + end + "]";
    }

    /**
     * Computes the smallest range that contains both this instance as well as the given {@code other} range.
     *
     * @param other other range or {@code null} in which case this instance is returned
     */
    public ByteRange minEnvelope(@Nullable ByteRange other) {
        if (other == null) {
            return this;
        }
        if (other.isSubRangeOf(this)) {
            return this;
        }
        if (this.isSubRangeOf(other)) {
            return other;
        }
        return of(Math.min(start, other.start), Math.max(end, other.end));
    }

    public long start() {
        return start;
    }

    public long end() {
        return end;
    }

    public long length() {
        return end - start;
    }

    /**
     * Checks if this instance is fully contained in the given {@code range}.
     */
    public boolean isSubRangeOf(ByteRange range) {
        return start >= range.start() && end <= range.end();
    }

    public boolean hasOverlap(ByteRange other) {
        return start < other.end && end > other.start;
    }

    public ByteRange overlap(ByteRange other) {
        return of(Math.max(start, other.start()), Math.min(end, other.end()));
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(start) + Long.hashCode(end);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ByteRange == false) {
            return false;
        }
        final ByteRange that = (ByteRange) obj;
        return start == that.start && end == that.end;
    }

    @Override
    public String toString() {
        return "ByteRange{" + start + "}{" + end + "}";
    }

    @Override
    public int compareTo(ByteRange o) {
        return Long.compare(start, o.start);
    }
}
