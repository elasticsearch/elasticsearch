/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.elasticsearch.index.shard.ShardLongFieldRange.LONG_FIELD_RANGE_VERSION_INTRODUCED;

/**
 * Class representing an (inclusive) range of {@code long} values in a field in an index which may comprise multiple shards. This
 * information is accumulated shard-by-shard, and we keep track of which shards are represented in this value. Only once all shards are
 * represented should this information be considered accurate for the index.
 */
public class IndexLongFieldRange implements Writeable, ToXContentFragment {

    /**
     * Sentinel value indicating that no information is currently available, for instance because the index has just been created.
     */
    public static final IndexLongFieldRange UNKNOWN = new IndexLongFieldRange(new int[0], Long.MAX_VALUE, Long.MIN_VALUE);

    /**
     * Sentinel value indicating an empty range, for instance because the field is missing or has no values in any shard.
     */
    public static final IndexLongFieldRange EMPTY = new IndexLongFieldRange(null, Long.MAX_VALUE, Long.MIN_VALUE);

    /**
     * Sentinel value indicating the actual range may change in the future.
     */
    public static final IndexLongFieldRange MUTABLE = new IndexLongFieldRange(null, Long.MIN_VALUE, Long.MAX_VALUE);

    @Nullable // if this range includes all shards
    private final int[] shards;
    private final long min, max;

    private IndexLongFieldRange(int[] shards, long min, long max) {
        assert (min == Long.MAX_VALUE && max == Long.MIN_VALUE) || min <= max : min + " vs " + max;
        assert shards == null || shards.length > 0 || (min == Long.MAX_VALUE && max == Long.MIN_VALUE) : Arrays.toString(shards);
        assert shards == null || Arrays.equals(shards, Arrays.stream(shards).sorted().distinct().toArray()) : Arrays.toString(shards);
        this.shards = shards;
        this.min = min;
        this.max = max;
    }

    /**
     * @return whether this range includes information from all shards yet.
     */
    public boolean isComplete() {
        return shards == null;
    }

    // exposed for testing
    int[] getShards() {
        return shards;
    }

    // exposed for testing
    long getMinUnsafe() {
        return min;
    }

    // exposed for testing
    long getMaxUnsafe() {
        return max;
    }

    /**
     * @return the (inclusive) minimum of this range.
     */
    public long getMin() {
        assert shards == null : "min is meaningless if we don't have data from all shards yet";
        assert this != EMPTY : "min is meaningless if range is empty";
        assert this != MUTABLE : "min is meaningless if range is mutable";
        return min;
    }

    /**
     * @return the (inclusive) maximum of this range.
     */
    public long getMax() {
        assert shards == null : "max is meaningless if we don't have data from all shards yet";
        assert this != EMPTY : "max is meaningless if range is empty";
        assert this != MUTABLE : "max is meaningless if range is mutable";
        return max;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(LONG_FIELD_RANGE_VERSION_INTRODUCED)) {
            if (this == UNKNOWN) {
                out.writeByte((byte) 1);
            } else if (this == MUTABLE) {
                out.writeByte((byte) 2);
            } else if (this == EMPTY) {
                out.writeByte((byte) 3);
            } else {
                out.writeByte((byte) 0);
                if (shards == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    out.writeVIntArray(shards);
                }
                out.writeZLong(min);
                out.writeZLong(max);
            }
        }
    }

    public static IndexLongFieldRange readFrom(StreamInput in) throws IOException {
        if (in.getVersion().before(LONG_FIELD_RANGE_VERSION_INTRODUCED)) {
            // conservative treatment for BWC
            return MUTABLE;
        }

        final byte type = in.readByte();
        switch (type) {
            case 1:
                return UNKNOWN;
            case 2:
                return MUTABLE;
            case 3:
                return EMPTY;
            case 0:
                return new IndexLongFieldRange(in.readBoolean() ? in.readVIntArray() : null, in.readZLong(), in.readZLong());
            default:
                throw new IllegalStateException("type [" + type + "] not known");
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (this == MUTABLE) {
            builder.field("mutable", true);
        } else if (this == EMPTY) {
            builder.field("empty", true);
        } else if (this == UNKNOWN) {
            builder.startArray("shards");
            builder.endArray();
        } else {
            builder.field("min", min);
            builder.field("max", max);
            if (shards != null) {
                builder.startArray("shards");
                for (int shard : shards) {
                    builder.value(shard);
                }
                builder.endArray();
            }
        }
        return builder;
    }

    public static IndexLongFieldRange fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        Boolean isMutable = null;
        Boolean isEmpty = null;
        Long min = null;
        Long max = null;
        List<Integer> shardsList = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("mutable".equals(currentFieldName)) {
                    if (Boolean.FALSE.equals(isMutable)) {
                        throw new IllegalArgumentException("unexpected field 'mutable'");
                    } else {
                        isMutable = Boolean.TRUE;
                        isEmpty = Boolean.FALSE;
                    }
                } else if ("empty".equals(currentFieldName)) {
                    if (Boolean.FALSE.equals(isEmpty)) {
                        throw new IllegalArgumentException("unexpected field 'empty'");
                    } else {
                        isMutable = Boolean.FALSE;
                        isEmpty = Boolean.TRUE;
                    }
                } else if ("min".equals(currentFieldName)) {
                    if (Boolean.TRUE.equals(isMutable) || Boolean.TRUE.equals(isEmpty)) {
                        throw new IllegalArgumentException("unexpected field 'min'");
                    } else {
                        isMutable = Boolean.FALSE;
                        isEmpty = Boolean.FALSE;
                        min = parser.longValue();
                    }
                } else if ("max".equals(currentFieldName)) {
                    if (Boolean.TRUE.equals(isMutable) || Boolean.TRUE.equals(isEmpty)) {
                        throw new IllegalArgumentException("unexpected field 'max'");
                    } else {
                        isMutable = Boolean.FALSE;
                        isEmpty = Boolean.FALSE;
                        max = parser.longValue();
                    }
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("shards".equals(currentFieldName)) {
                    if (Boolean.TRUE.equals(isMutable) || Boolean.TRUE.equals(isEmpty) || shardsList != null) {
                        throw new IllegalArgumentException("unexpected array 'shards'");
                    } else {
                        isMutable = Boolean.FALSE;
                        isEmpty = Boolean.FALSE;
                        shardsList = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token.isValue()) {
                                shardsList.add(parser.intValue());
                            }
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected array: " + currentFieldName);
                }
            } else {
                throw new IllegalArgumentException("Unexpected token: " + token);
            }
        }

        if (Boolean.TRUE.equals(isMutable)) {
            //noinspection ConstantConditions this assertion is always true but left here for the benefit of readers
            assert min == null && max == null && shardsList == null && Boolean.FALSE.equals(isEmpty);
            return MUTABLE;
        } else if (Boolean.TRUE.equals(isEmpty)) {
            //noinspection ConstantConditions this assertion is always true but left here for the benefit of readers
            assert min == null && max == null && shardsList == null && Boolean.FALSE.equals(isMutable);
            return EMPTY;
        } else if (shardsList != null && shardsList.isEmpty()) {
            //noinspection ConstantConditions this assertion is always true but left here for the benefit of readers
            assert min == null && max == null && Boolean.FALSE.equals(isEmpty) && Boolean.FALSE.equals(isMutable);
            return UNKNOWN;
        } else if (min != null) {
            //noinspection ConstantConditions this assertion is always true but left here for the benefit of readers
            assert Boolean.FALSE.equals(isMutable) && Boolean.FALSE.equals(isEmpty);
            if (max == null) {
                throw new IllegalArgumentException("field 'max' unexpectedly missing");
            }
            final int[] shards;
            if (shardsList != null) {
                shards = shardsList.stream().mapToInt(i -> i).toArray();
                assert shards.length > 0;
            } else {
                shards = null;
            }
            return new IndexLongFieldRange(shards, min, max);
        } else {
            throw new IllegalArgumentException("field range contents unexpectedly missing");
        }
    }

    public IndexLongFieldRange extendWithShardRange(int shardId, int shardCount, ShardLongFieldRange shardFieldRange) {
        if (shardFieldRange == ShardLongFieldRange.MUTABLE) {
            assert shards == null
                    ? this == MUTABLE
                    : Arrays.stream(shards).noneMatch(i -> i == shardId) : Arrays.toString(shards);
            return MUTABLE;
        }
        if (shards == null || Arrays.stream(shards).anyMatch(i -> i == shardId)) {
            assert shardFieldRange == ShardLongFieldRange.EMPTY || min <= shardFieldRange.getMin() && shardFieldRange.getMax() <= max;
            return this;
        }
        final int[] newShards;
        if (shards.length == shardCount - 1) {
            assert Arrays.equals(shards, IntStream.range(0, shardCount).filter(i -> i != shardId).toArray())
                    : Arrays.toString(shards) + " + " + shardId;
            if (shardFieldRange == ShardLongFieldRange.EMPTY && min == EMPTY.min && max == EMPTY.max) {
                return EMPTY;
            }
            newShards = null;
        } else {
            newShards = IntStream.concat(Arrays.stream(this.shards), IntStream.of(shardId)).sorted().toArray();
        }
        if (shardFieldRange == ShardLongFieldRange.EMPTY) {
            return new IndexLongFieldRange(newShards, min, max);
        } else {
            return new IndexLongFieldRange(newShards, Math.min(shardFieldRange.getMin(), min), Math.max(shardFieldRange.getMax(), max));
        }
    }

    @Override
    public String toString() {
        if (this == UNKNOWN) {
            return "UNKNOWN";
        } else if (this == MUTABLE) {
            return "MUTABLE";
        } else if (this == EMPTY) {
            return "EMPTY";
        } else if (shards == null) {
            return "[" + min + "-" + max + "]";
        } else {
            return "[" + min + "-" + max + ", shards=" + Arrays.toString(shards) + "]";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (this == EMPTY || this == MUTABLE || this == UNKNOWN || o == EMPTY || o == MUTABLE || o == UNKNOWN) return false;
        IndexLongFieldRange that = (IndexLongFieldRange) o;
        return min == that.min &&
                max == that.max &&
                Arrays.equals(shards, that.shards);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(min, max);
        result = 31 * result + Arrays.hashCode(shards);
        return result;
    }

    /**
     * Remove the given shard from the set of known shards, possibly without adjusting the min and max. Used when allocating a stale primary
     * which may have a different range from the original, so we must allow the range to grow. Note that this doesn't usually allow the
     * range to shrink, so we may in theory hit this shard more than needed after allocating a stale primary.
     */
    public IndexLongFieldRange removeShard(int shardId, int numberOfShards) {
        assert 0 <= shardId && shardId < numberOfShards : shardId + " vs " + numberOfShards;

        if (shards != null && Arrays.stream(shards).noneMatch(i -> i == shardId)) {
            return this;
        }
        if (this == MUTABLE) {
            return this;
        }

        if (shards == null && numberOfShards == 1) {
            return UNKNOWN;
        }
        if (shards != null && shards.length == 1 && shards[0] == shardId) {
            return UNKNOWN;
        }

        final IntStream currentShards = shards == null ? IntStream.range(0, numberOfShards) : Arrays.stream(shards);
        return new IndexLongFieldRange(currentShards.filter(i -> i != shardId).toArray(), min, max);
    }
}
