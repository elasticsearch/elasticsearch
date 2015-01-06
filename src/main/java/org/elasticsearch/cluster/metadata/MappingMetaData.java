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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.TimestampParsingException;
import org.elasticsearch.cluster.AbstractClusterStatePart;
import org.elasticsearch.cluster.ClusterStatePart;
import org.elasticsearch.cluster.LocalContext;
import org.elasticsearch.cluster.NamedClusterStatePart;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Mapping configuration for a type.
 */
public class MappingMetaData extends AbstractClusterStatePart implements NamedClusterStatePart, IndexClusterStatePart<MappingMetaData> {

    public static final String TYPE = "mapping";

    public static final Factory FACTORY = new Factory();

    @Override
    public String partType() {
        return TYPE;
    }

    @Override
    public String key() {
        return type;
    }

    @Override
    public MappingMetaData mergeWith(MappingMetaData second) {
        return null;
    }

    public static class Factory extends AbstractClusterStatePart.AbstractFactory<MappingMetaData> {

        @Override
        public MappingMetaData readFrom(StreamInput in, LocalContext context) throws IOException {
            return MappingMetaData.readFrom(in);
        }

        @Override
        public void writeTo(MappingMetaData part, StreamOutput out) throws IOException {
            MappingMetaData.writeTo(part, out);
        }

        @Override
        public String partType() {
            return TYPE;
        }

        @Override
        public void toXContent(MappingMetaData part, XContentBuilder builder, ToXContent.Params params) throws IOException {
            if(params.paramAsBoolean("reduce_mappings", false)) {
                byte[] mappingSource = part.source().uncompressed();
                XContentParser parser = XContentFactory.xContent(mappingSource).createParser(mappingSource);
                Map<String, Object> mapping = parser.map();
                if (mapping.size() == 1 && mapping.containsKey(partType())) {
                    // the type name is the root value, reduce it
                    mapping = (Map<String, Object>) mapping.get(partType());
                }
                builder.field(partType());
                builder.map(mapping);
            } else if (params.paramAsBoolean("binary", false)) {
                builder.value(part.source().compressed());
            } else {
                byte[] data = part.source().uncompressed();
                XContentParser parser = XContentFactory.xContent(data).createParser(data);
                Map<String, Object> mapping = parser.mapOrdered();
                parser.close();
                builder.map(mapping);
            }
        }
    }

    public static class Id {

        public static final Id EMPTY = new Id(null);

        private final String path;

        private final String[] pathElements;

        public Id(String path) {
            this.path = path;
            if (path == null) {
                pathElements = Strings.EMPTY_ARRAY;
            } else {
                pathElements = Strings.delimitedListToStringArray(path, ".");
            }
        }

        public boolean hasPath() {
            return path != null;
        }

        public String path() {
            return this.path;
        }

        public String[] pathElements() {
            return this.pathElements;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Id id = (Id) o;

            if (path != null ? !path.equals(id.path) : id.path != null) return false;
            if (!Arrays.equals(pathElements, id.pathElements)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = path != null ? path.hashCode() : 0;
            result = 31 * result + (pathElements != null ? Arrays.hashCode(pathElements) : 0);
            return result;
        }
    }

    public static class Routing {

        public static final Routing EMPTY = new Routing(false, null);

        private final boolean required;

        private final String path;

        private final String[] pathElements;

        public Routing(boolean required, String path) {
            this.required = required;
            this.path = path;
            if (path == null) {
                pathElements = Strings.EMPTY_ARRAY;
            } else {
                pathElements = Strings.delimitedListToStringArray(path, ".");
            }
        }

        public boolean required() {
            return required;
        }

        public boolean hasPath() {
            return path != null;
        }

        public String path() {
            return this.path;
        }

        public String[] pathElements() {
            return this.pathElements;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Routing routing = (Routing) o;

            if (required != routing.required) return false;
            if (path != null ? !path.equals(routing.path) : routing.path != null) return false;
            if (!Arrays.equals(pathElements, routing.pathElements)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (required ? 1 : 0);
            result = 31 * result + (path != null ? path.hashCode() : 0);
            result = 31 * result + (pathElements != null ? Arrays.hashCode(pathElements) : 0);
            return result;
        }
    }

    public static class Timestamp {

        public static String parseStringTimestamp(String timestampAsString, FormatDateTimeFormatter dateTimeFormatter) throws TimestampParsingException {
            long ts;
            try {
                // if we manage to parse it, its a millisecond timestamp, just return the string as is
                ts = Long.parseLong(timestampAsString);
                return timestampAsString;
            } catch (NumberFormatException e) {
                try {
                    ts = dateTimeFormatter.parser().parseMillis(timestampAsString);
                } catch (RuntimeException e1) {
                    throw new TimestampParsingException(timestampAsString);
                }
            }
            return Long.toString(ts);
        }


        public static final Timestamp EMPTY = new Timestamp(false, null, TimestampFieldMapper.DEFAULT_DATE_TIME_FORMAT,
                TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP);

        private final boolean enabled;

        private final String path;

        private final String format;

        private final String[] pathElements;

        private final FormatDateTimeFormatter dateTimeFormatter;

        private final String defaultTimestamp;

        public Timestamp(boolean enabled, String path, String format, String defaultTimestamp) {
            this.enabled = enabled;
            this.path = path;
            if (path == null) {
                pathElements = Strings.EMPTY_ARRAY;
            } else {
                pathElements = Strings.delimitedListToStringArray(path, ".");
            }
            this.format = format;
            this.dateTimeFormatter = Joda.forPattern(format);
            this.defaultTimestamp = defaultTimestamp;
        }

        public boolean enabled() {
            return enabled;
        }

        public boolean hasPath() {
            return path != null;
        }

        public String path() {
            return this.path;
        }

        public String[] pathElements() {
            return this.pathElements;
        }

        public String format() {
            return this.format;
        }

        public String defaultTimestamp() {
            return this.defaultTimestamp;
        }

        public boolean hasDefaultTimestamp() {
            return this.defaultTimestamp != null;
        }

        public FormatDateTimeFormatter dateTimeFormatter() {
            return this.dateTimeFormatter;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Timestamp timestamp = (Timestamp) o;

            if (enabled != timestamp.enabled) return false;
            if (format != null ? !format.equals(timestamp.format) : timestamp.format != null) return false;
            if (path != null ? !path.equals(timestamp.path) : timestamp.path != null) return false;
            if (defaultTimestamp != null ? !defaultTimestamp.equals(timestamp.defaultTimestamp) : timestamp.defaultTimestamp != null) return false;
            if (!Arrays.equals(pathElements, timestamp.pathElements)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (enabled ? 1 : 0);
            result = 31 * result + (path != null ? path.hashCode() : 0);
            result = 31 * result + (format != null ? format.hashCode() : 0);
            result = 31 * result + (pathElements != null ? Arrays.hashCode(pathElements) : 0);
            result = 31 * result + (dateTimeFormatter != null ? dateTimeFormatter.hashCode() : 0);
            result = 31 * result + (defaultTimestamp != null ? defaultTimestamp.hashCode() : 0);
            return result;
        }
    }

    private final String type;

    private final CompressedString source;

    private Id id;
    private Routing routing;
    private Timestamp timestamp;
    private boolean hasParentField;

    public MappingMetaData(DocumentMapper docMapper) {
        this.type = docMapper.type();
        this.source = docMapper.mappingSource();
        this.id = new Id(docMapper.idFieldMapper().path());
        this.routing = new Routing(docMapper.routingFieldMapper().required(), docMapper.routingFieldMapper().path());
        this.timestamp = new Timestamp(docMapper.timestampFieldMapper().enabled(), docMapper.timestampFieldMapper().path(), docMapper.timestampFieldMapper().dateTimeFormatter().format(), docMapper.timestampFieldMapper().defaultTimestamp());
        this.hasParentField = docMapper.parentFieldMapper().active();
    }

    public MappingMetaData(CompressedString mapping) throws IOException {
        this.source = mapping;
        Map<String, Object> mappingMap = XContentHelper.createParser(mapping.compressed(), 0, mapping.compressed().length).mapOrderedAndClose();
        if (mappingMap.size() != 1) {
            throw new ElasticsearchIllegalStateException("Can't derive type from mapping, no root type: " + mapping.string());
        }
        this.type = mappingMap.keySet().iterator().next();
        initMappers((Map<String, Object>) mappingMap.get(this.type));
    }

    public MappingMetaData(Map<String, Object> mapping) throws IOException {
        this(mapping.keySet().iterator().next(), mapping);
    }

    public MappingMetaData(String type, Map<String, Object> mapping) throws IOException {
        this.type = type;
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().map(mapping);
        this.source = new CompressedString(mappingBuilder.bytes());
        Map<String, Object> withoutType = mapping;
        if (mapping.size() == 1 && mapping.containsKey(type)) {
            withoutType = (Map<String, Object>) mapping.get(type);
        }
        initMappers(withoutType);
    }

    private void initMappers(Map<String, Object> withoutType) {
        if (withoutType.containsKey("_id")) {
            String path = null;
            Map<String, Object> routingNode = (Map<String, Object>) withoutType.get("_id");
            for (Map.Entry<String, Object> entry : routingNode.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("path")) {
                    path = fieldNode.toString();
                }
            }
            this.id = new Id(path);
        } else {
            this.id = Id.EMPTY;
        }
        if (withoutType.containsKey("_routing")) {
            boolean required = false;
            String path = null;
            Map<String, Object> routingNode = (Map<String, Object>) withoutType.get("_routing");
            for (Map.Entry<String, Object> entry : routingNode.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("required")) {
                    required = nodeBooleanValue(fieldNode);
                } else if (fieldName.equals("path")) {
                    path = fieldNode.toString();
                }
            }
            this.routing = new Routing(required, path);
        } else {
            this.routing = Routing.EMPTY;
        }
        if (withoutType.containsKey("_timestamp")) {
            boolean enabled = false;
            String path = null;
            String format = TimestampFieldMapper.DEFAULT_DATE_TIME_FORMAT;
            String defaultTimestamp = TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP;
            Map<String, Object> timestampNode = (Map<String, Object>) withoutType.get("_timestamp");
            for (Map.Entry<String, Object> entry : timestampNode.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    enabled = nodeBooleanValue(fieldNode);
                } else if (fieldName.equals("path")) {
                    path = fieldNode.toString();
                } else if (fieldName.equals("format")) {
                    format = fieldNode.toString();
                } else if (fieldName.equals("default")) {
                    defaultTimestamp = fieldNode.toString();
                }
            }
            this.timestamp = new Timestamp(enabled, path, format, defaultTimestamp);
        } else {
            this.timestamp = Timestamp.EMPTY;
        }
        if (withoutType.containsKey("_parent")) {
            this.hasParentField = true;
        } else {
            this.hasParentField = false;
        }
    }

    public MappingMetaData(String type, CompressedString source, Id id, Routing routing, Timestamp timestamp, boolean hasParentField) {
        this.type = type;
        this.source = source;
        this.id = id;
        this.routing = routing;
        this.timestamp = timestamp;
        this.hasParentField = hasParentField;
    }

    void updateDefaultMapping(MappingMetaData defaultMapping) {
        if (id == Id.EMPTY) {
            id = defaultMapping.id();
        }
        if (routing == Routing.EMPTY) {
            routing = defaultMapping.routing();
        }
        if (timestamp == Timestamp.EMPTY) {
            timestamp = defaultMapping.timestamp();
        }
    }

    public String type() {
        return this.type;
    }

    public CompressedString source() {
        return this.source;
    }

    public boolean hasParentField() {
        return hasParentField;
    }

    /**
     * Converts the serialized compressed form of the mappings into a parsed map.
     */
    public Map<String, Object> sourceAsMap() throws IOException {
        Map<String, Object> mapping = XContentHelper.convertToMap(source.compressed(), 0, source.compressed().length, true).v2();
        if (mapping.size() == 1 && mapping.containsKey(type())) {
            // the type name is the root value, reduce it
            mapping = (Map<String, Object>) mapping.get(type());
        }
        return mapping;
    }

    /**
     * Converts the serialized compressed form of the mappings into a parsed map.
     */
    public Map<String, Object> getSourceAsMap() throws IOException {
        return sourceAsMap();
    }

    public Id id() {
        return this.id;
    }

    public Routing routing() {
        return this.routing;
    }

    public Timestamp timestamp() {
        return this.timestamp;
    }

    public ParseContext createParseContext(@Nullable String id, @Nullable String routing, @Nullable String timestamp) {
        // We parse the routing even if there is already a routing key in the request in order to make sure that
        // they are the same
        return new ParseContext(
                id == null && id().hasPath(),
                routing().hasPath(),
                timestamp == null && timestamp().hasPath()
        );
    }

    public void parse(XContentParser parser, ParseContext parseContext) throws IOException {
        innerParse(parser, parseContext);
    }

    private void innerParse(XContentParser parser, ParseContext context) throws IOException {
        if (!context.parsingStillNeeded()) {
            return;
        }

        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        String idPart = context.idParsingStillNeeded() ? id().pathElements()[context.locationId] : null;
        String routingPart = context.routingParsingStillNeeded() ? routing().pathElements()[context.locationRouting] : null;
        String timestampPart = context.timestampParsingStillNeeded() ? timestamp().pathElements()[context.locationTimestamp] : null;

        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            // Must point to field name
            String fieldName = parser.currentName();
            // And then the value...
            token = parser.nextToken();
            boolean incLocationId = false;
            boolean incLocationRouting = false;
            boolean incLocationTimestamp = false;
            if (context.idParsingStillNeeded() && fieldName.equals(idPart)) {
                if (context.locationId + 1 == id.pathElements().length) {
                    if (!token.isValue()) {
                        throw new MapperParsingException("id field must be a value but was either an object or an array");
                    }
                    context.id = parser.textOrNull();
                    context.idResolved = true;
                } else {
                    incLocationId = true;
                }
            }
            if (context.routingParsingStillNeeded() && fieldName.equals(routingPart)) {
                if (context.locationRouting + 1 == routing.pathElements().length) {
                    context.routing = parser.textOrNull();
                    context.routingResolved = true;
                } else {
                    incLocationRouting = true;
                }
            }
            if (context.timestampParsingStillNeeded() && fieldName.equals(timestampPart)) {
                if (context.locationTimestamp + 1 == timestamp.pathElements().length) {
                    context.timestamp = parser.textOrNull();
                    context.timestampResolved = true;
                } else {
                    incLocationTimestamp = true;
                }
            }

            if (incLocationId || incLocationRouting || incLocationTimestamp) {
                if (token == XContentParser.Token.START_OBJECT) {
                    context.locationId += incLocationId ? 1 : 0;
                    context.locationRouting += incLocationRouting ? 1 : 0;
                    context.locationTimestamp += incLocationTimestamp ? 1 : 0;
                    innerParse(parser, context);
                    context.locationId -= incLocationId ? 1 : 0;
                    context.locationRouting -= incLocationRouting ? 1 : 0;
                    context.locationTimestamp -= incLocationTimestamp ? 1 : 0;
                }
            } else {
                parser.skipChildren();
            }

            if (!context.parsingStillNeeded()) {
                return;
            }
        }
    }

    public static void writeTo(MappingMetaData mappingMd, StreamOutput out) throws IOException {
        out.writeString(mappingMd.type());
        mappingMd.source().writeTo(out);
        // id
        if (mappingMd.id().hasPath()) {
            out.writeBoolean(true);
            out.writeString(mappingMd.id().path());
        } else {
            out.writeBoolean(false);
        }
        // routing
        out.writeBoolean(mappingMd.routing().required());
        if (mappingMd.routing().hasPath()) {
            out.writeBoolean(true);
            out.writeString(mappingMd.routing().path());
        } else {
            out.writeBoolean(false);
        }
        // timestamp
        out.writeBoolean(mappingMd.timestamp().enabled());
        out.writeOptionalString(mappingMd.timestamp().path());
        out.writeString(mappingMd.timestamp().format());
        out.writeOptionalString(mappingMd.timestamp().defaultTimestamp());
        out.writeBoolean(mappingMd.hasParentField());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingMetaData that = (MappingMetaData) o;

        if (!id.equals(that.id)) return false;
        if (!routing.equals(that.routing)) return false;
        if (!source.equals(that.source)) return false;
        if (!timestamp.equals(that.timestamp)) return false;
        if (!type.equals(that.type)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + source.hashCode();
        result = 31 * result + id.hashCode();
        result = 31 * result + routing.hashCode();
        result = 31 * result + timestamp.hashCode();
        return result;
    }

    public static MappingMetaData readFrom(StreamInput in) throws IOException {
        String type = in.readString();
        CompressedString source = CompressedString.readCompressedString(in);
        // id
        Id id = new Id(in.readBoolean() ? in.readString() : null);
        // routing
        Routing routing = new Routing(in.readBoolean(), in.readBoolean() ? in.readString() : null);
        // timestamp
        final Timestamp timestamp = new Timestamp(in.readBoolean(), in.readOptionalString(), in.readString(), in.readOptionalString());
        final boolean hasParentField = in.readBoolean();
        return new MappingMetaData(type, source, id, routing, timestamp, hasParentField);
    }

    public static class ParseContext {
        final boolean shouldParseId;
        final boolean shouldParseRouting;
        final boolean shouldParseTimestamp;

        int locationId = 0;
        int locationRouting = 0;
        int locationTimestamp = 0;
        boolean idResolved;
        boolean routingResolved;
        boolean timestampResolved;
        String id;
        String routing;
        String timestamp;

        public ParseContext(boolean shouldParseId, boolean shouldParseRouting, boolean shouldParseTimestamp) {
            this.shouldParseId = shouldParseId;
            this.shouldParseRouting = shouldParseRouting;
            this.shouldParseTimestamp = shouldParseTimestamp;
        }

        /**
         * The id value parsed, <tt>null</tt> if does not require parsing, or not resolved.
         */
        public String id() {
            return id;
        }

        /**
         * Does id parsing really needed at all?
         */
        public boolean shouldParseId() {
            return shouldParseId;
        }

        /**
         * Has id been resolved during the parsing phase.
         */
        public boolean idResolved() {
            return idResolved;
        }

        /**
         * Is id parsing still needed?
         */
        public boolean idParsingStillNeeded() {
            return shouldParseId && !idResolved;
        }

        /**
         * The routing value parsed, <tt>null</tt> if does not require parsing, or not resolved.
         */
        public String routing() {
            return routing;
        }

        /**
         * Does routing parsing really needed at all?
         */
        public boolean shouldParseRouting() {
            return shouldParseRouting;
        }

        /**
         * Has routing been resolved during the parsing phase.
         */
        public boolean routingResolved() {
            return routingResolved;
        }

        /**
         * Is routing parsing still needed?
         */
        public boolean routingParsingStillNeeded() {
            return shouldParseRouting && !routingResolved;
        }

        /**
         * The timestamp value parsed, <tt>null</tt> if does not require parsing, or not resolved.
         */
        public String timestamp() {
            return timestamp;
        }

        /**
         * Does timestamp parsing really needed at all?
         */
        public boolean shouldParseTimestamp() {
            return shouldParseTimestamp;
        }

        /**
         * Has timestamp been resolved during the parsing phase.
         */
        public boolean timestampResolved() {
            return timestampResolved;
        }

        /**
         * Is timestamp parsing still needed?
         */
        public boolean timestampParsingStillNeeded() {
            return shouldParseTimestamp && !timestampResolved;
        }

        /**
         * Do we really need parsing?
         */
        public boolean shouldParse() {
            return shouldParseId || shouldParseRouting || shouldParseTimestamp;
        }

        /**
         * Is parsing still needed?
         */
        public boolean parsingStillNeeded() {
            return idParsingStillNeeded() || routingParsingStillNeeded() || timestampParsingStillNeeded();
        }
    }
}
