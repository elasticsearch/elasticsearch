/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.SerializableString;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * A base class for the response of a write operation that involves a single doc
 */
public abstract class DocWriteResponse extends ReplicationResponse implements WriteResponse, ToXContentObject {

    private static final String _SHARDS = "_shards";
    private static final SerializableString _SHARDS_FIELD = SerializableString.of(_SHARDS);
    private static final String _INDEX = "_index";
    private static final SerializableString _INDEX_FIELD = SerializableString.of(_INDEX);
    private static final String _ID = "_id";
    private static final SerializableString _ID_FIELD = SerializableString.of(_ID);
    private static final String _VERSION = "_version";
    private static final SerializableString _VERSION_FIELD = SerializableString.of(_VERSION);
    private static final String _SEQ_NO = "_seq_no";
    private static final SerializableString _SEQ_NO_FIELD = SerializableString.of(_SEQ_NO);
    private static final String _PRIMARY_TERM = "_primary_term";
    private static final SerializableString _PRIMARY_TERM_FIELD = SerializableString.of(_PRIMARY_TERM);
    private static final String RESULT = "result";
    private static final SerializableString RESULT_FIELD = SerializableString.of(RESULT);
    private static final String FORCED_REFRESH = "forced_refresh";
    private static final SerializableString FORCED_REFRESH_FIELD = SerializableString.of(FORCED_REFRESH);

    private static final Map<Result, SerializableString> RESULT_MAP = Arrays.stream(Result.values())
        .collect(Collectors.toUnmodifiableMap(r -> r, r -> SerializableString.of(r.getLowercase())));

    /**
     * An enum that represents the results of CRUD operations, primarily used to communicate the type of
     * operation that occurred.
     */
    public enum Result implements Writeable {
        CREATED(0),
        UPDATED(1),
        DELETED(2),
        NOT_FOUND(3),
        NOOP(4);

        private final byte op;
        private final String lowercase;

        Result(int op) {
            this.op = (byte) op;
            this.lowercase = this.name().toLowerCase(Locale.ROOT);
        }

        public byte getOp() {
            return op;
        }

        public String getLowercase() {
            return lowercase;
        }

        public static Result readFrom(StreamInput in) throws IOException {
            Byte opcode = in.readByte();
            return switch (opcode) {
                case 0 -> CREATED;
                case 1 -> UPDATED;
                case 2 -> DELETED;
                case 3 -> NOT_FOUND;
                case 4 -> NOOP;
                default -> throw new IllegalArgumentException("Unknown result code: " + opcode);
            };
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(op);
        }
    }

    private final ShardId shardId;
    private final String id;
    private final long version;
    private final long seqNo;
    private final long primaryTerm;
    private boolean forcedRefresh;
    protected final Result result;

    public DocWriteResponse(ShardId shardId, String id, long seqNo, long primaryTerm, long version, Result result) {
        this.shardId = Objects.requireNonNull(shardId);
        this.id = Objects.requireNonNull(id);
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.version = version;
        this.result = Objects.requireNonNull(result);
    }

    // needed for deserialization
    protected DocWriteResponse(ShardId shardId, StreamInput in) throws IOException {
        super(in);
        this.shardId = shardId;
        if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            String type = in.readString();
            assert MapperService.SINGLE_MAPPING_NAME.equals(type) : "Expected [_doc] but received [" + type + "]";
        }
        id = in.readString();
        version = in.readZLong();
        seqNo = in.readZLong();
        primaryTerm = in.readVLong();
        forcedRefresh = in.readBoolean();
        result = Result.readFrom(in);
    }

    /**
     * Needed for deserialization of single item requests in {@link org.elasticsearch.action.index.TransportIndexAction} and BwC
     * deserialization path
     */
    protected DocWriteResponse(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            String type = in.readString();
            assert MapperService.SINGLE_MAPPING_NAME.equals(type) : "Expected [_doc] but received [" + type + "]";
        }
        id = in.readString();
        version = in.readZLong();
        seqNo = in.readZLong();
        primaryTerm = in.readVLong();
        forcedRefresh = in.readBoolean();
        result = Result.readFrom(in);
    }

    /**
     * The change that occurred to the document.
     */
    public Result getResult() {
        return result;
    }

    /**
     * The index the document was changed in.
     */
    public String getIndex() {
        return this.shardId.getIndexName();
    }

    /**
     * The exact shard the document was changed in.
     */
    public ShardId getShardId() {
        return this.shardId;
    }

    /**
     * The id of the document changed.
     */
    public String getId() {
        return this.id;
    }

    /**
     * Returns the current version of the doc.
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * Returns the sequence number assigned for this change. Returns {@link SequenceNumbers#UNASSIGNED_SEQ_NO} if the operation
     * wasn't performed (i.e., an update operation that resulted in a NOOP).
     */
    public long getSeqNo() {
        return seqNo;
    }

    /**
     * The primary term for this change.
     *
     * @return the primary term
     */
    public long getPrimaryTerm() {
        return primaryTerm;
    }

    /**
     * Did this request force a refresh? Requests that set {@link WriteRequest#setRefreshPolicy(RefreshPolicy)} to
     * {@link RefreshPolicy#IMMEDIATE} will always return true for this. Requests that set it to {@link RefreshPolicy#WAIT_UNTIL} will
     * only return true here if they run out of refresh listener slots (see {@link IndexSettings#MAX_REFRESH_LISTENERS_PER_SHARD}).
     */
    public boolean forcedRefresh() {
        return forcedRefresh;
    }

    @Override
    public void setForcedRefresh(boolean forcedRefresh) {
        this.forcedRefresh = forcedRefresh;
    }

    /** returns the rest status for this response (based on {@link ShardInfo#status()} */
    public RestStatus status() {
        return getShardInfo().status();
    }

    /**
     * Return the relative URI for the location of the document suitable for use in the {@code Location} header. The use of relative URIs is
     * permitted as of HTTP/1.1 (cf. https://tools.ietf.org/html/rfc7231#section-7.1.2).
     *
     * @param routing custom routing or {@code null} if custom routing is not used
     * @return the relative URI for the location of the document
     */
    public String getLocation(@Nullable String routing) {
        // encode the path components separately otherwise the path separators will be encoded
        final String encodedIndex = URLEncoder.encode(getIndex(), StandardCharsets.UTF_8);
        final String encodedType = URLEncoder.encode(MapperService.SINGLE_MAPPING_NAME, StandardCharsets.UTF_8);
        final String encodedId = URLEncoder.encode(getId(), StandardCharsets.UTF_8);
        final String encodedRouting = routing == null ? null : URLEncoder.encode(routing, StandardCharsets.UTF_8);
        final String routingStart = "?routing=";
        final int bufferSizeExcludingRouting = 3 + encodedIndex.length() + encodedType.length() + encodedId.length();
        final int bufferSize;
        if (encodedRouting == null) {
            bufferSize = bufferSizeExcludingRouting;
        } else {
            bufferSize = bufferSizeExcludingRouting + routingStart.length() + encodedRouting.length();
        }
        final StringBuilder location = new StringBuilder(bufferSize);
        location.append('/').append(encodedIndex);
        location.append('/').append(encodedType);
        location.append('/').append(encodedId);
        if (encodedRouting != null) {
            location.append(routingStart).append(encodedRouting);
        }

        return location.toString();
    }

    public void writeThin(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeWithoutShardId(out);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        writeWithoutShardId(out);
    }

    private void writeWithoutShardId(StreamOutput out) throws IOException {
        if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            out.writeString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeString(id);
        out.writeZLong(version);
        out.writeZLong(seqNo);
        out.writeVLong(primaryTerm);
        out.writeBoolean(forcedRefresh);
        result.writeTo(out);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        ReplicationResponse.ShardInfo shardInfo = getShardInfo();
        builder.field(_INDEX_FIELD, shardId.getIndexName());
        builder.field(_ID_FIELD, id);
        builder.field(_VERSION_FIELD).value(version);
        builder.field(RESULT_FIELD).value(RESULT_MAP.get(getResult()));
        if (forcedRefresh) {
            builder.field(FORCED_REFRESH_FIELD).value(true);
        }
        builder.field(_SHARDS_FIELD).value(shardInfo);
        if (getSeqNo() >= 0) {
            builder.field(_SEQ_NO_FIELD).value(getSeqNo());
            builder.field(_PRIMARY_TERM_FIELD).value(getPrimaryTerm());
        }
        if (builder.getRestApiVersion() == RestApiVersion.V_7) {
            builder.field(BulkItemResponse.TYPE_FIELD).value(BulkItemResponse.TYPE_VALUE);
        }
        return builder;
    }

    /**
     * Parse the output of the {@link #innerToXContent(XContentBuilder, Params)} method.
     *
     * This method is intended to be called by subclasses and must be called multiple times to parse all the information concerning
     * {@link DocWriteResponse} objects. It always parses the current token, updates the given parsing context accordingly
     * if needed and then immediately returns.
     */
    public static void parseInnerToXContent(XContentParser parser, Builder context) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);

        String currentFieldName = parser.currentName();
        token = parser.nextToken();

        if (token.isValue()) {
            if (_INDEX.equals(currentFieldName)) {
                // index uuid and shard id are unknown and can't be parsed back for now.
                context.setShardId(new ShardId(new Index(parser.text(), IndexMetadata.INDEX_UUID_NA_VALUE), -1));
            } else if (_ID.equals(currentFieldName)) {
                context.setId(parser.text());
            } else if (_VERSION.equals(currentFieldName)) {
                context.setVersion(parser.longValue());
            } else if (RESULT.equals(currentFieldName)) {
                String result = parser.text();
                for (Result r : Result.values()) {
                    if (r.getLowercase().equals(result)) {
                        context.setResult(r);
                        break;
                    }
                }
            } else if (FORCED_REFRESH.equals(currentFieldName)) {
                context.setForcedRefresh(parser.booleanValue());
            } else if (_SEQ_NO.equals(currentFieldName)) {
                context.setSeqNo(parser.longValue());
            } else if (_PRIMARY_TERM.equals(currentFieldName)) {
                context.setPrimaryTerm(parser.longValue());
            }
        } else if (token == XContentParser.Token.START_OBJECT) {
            if (_SHARDS.equals(currentFieldName)) {
                context.setShardInfo(ShardInfo.fromXContent(parser));
            } else {
                parser.skipChildren(); // skip potential inner objects for forward compatibility
            }
        } else if (token == XContentParser.Token.START_ARRAY) {
            parser.skipChildren(); // skip potential inner arrays for forward compatibility
        }
    }

    /**
     * Base class of all {@link DocWriteResponse} builders. These {@link DocWriteResponse.Builder} are used during
     * xcontent parsing to temporarily store the parsed values, then the {@link Builder#build()} method is called to
     * instantiate the appropriate {@link DocWriteResponse} with the parsed values.
     */
    public abstract static class Builder {

        protected ShardId shardId = null;
        protected String id = null;
        protected Long version = null;
        protected Result result = null;
        protected boolean forcedRefresh;
        protected ShardInfo shardInfo = null;
        protected long seqNo = UNASSIGNED_SEQ_NO;
        protected long primaryTerm = UNASSIGNED_PRIMARY_TERM;

        public ShardId getShardId() {
            return shardId;
        }

        public void setShardId(ShardId shardId) {
            this.shardId = shardId;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setVersion(Long version) {
            this.version = version;
        }

        public void setResult(Result result) {
            this.result = result;
        }

        public void setForcedRefresh(boolean forcedRefresh) {
            this.forcedRefresh = forcedRefresh;
        }

        public void setShardInfo(ShardInfo shardInfo) {
            this.shardInfo = shardInfo;
        }

        public void setSeqNo(long seqNo) {
            this.seqNo = seqNo;
        }

        public void setPrimaryTerm(long primaryTerm) {
            this.primaryTerm = primaryTerm;
        }

        public abstract DocWriteResponse build();
    }
}
