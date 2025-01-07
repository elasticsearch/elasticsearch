/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Block loader for fields that use fallback synthetic source implementation.
 * <br>
 * Usually fields have doc_values or stored fields and block loaders use them directly. In some cases neither is available
 * and we would fall back to (potentially synthetic) _source. However, in case of synthetic source, there is actually no need to
 * construct the entire _source. We know that there is no doc_values and stored fields, and therefore we will be using fallback synthetic
 * source. That is equivalent to just reading _ignored_source stored field directly and doing an in-place synthetic source just
 * for this field.
 * <br>
 * See {@link IgnoredSourceFieldMapper}.
 */
public abstract class FallbackSyntheticSourceBlockLoader implements BlockLoader {
    private final MappedFieldType.BlockLoaderContext blockLoaderContext;
    private final Reader reader;
    private final String fieldName;

    protected FallbackSyntheticSourceBlockLoader(MappedFieldType.BlockLoaderContext blockLoaderContext, Reader reader, String fieldName) {
        this.blockLoaderContext = blockLoaderContext;
        this.reader = reader;
        this.fieldName = fieldName;
    }

    @Override
    public ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) throws IOException {
        return null;
    }

    @Override
    public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
        return new IgnoredSourceRowStrideReader(fieldName, reader);
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        return new StoredFieldsSpec(false, false, Set.of(IgnoredSourceFieldMapper.NAME));
    }

    @Override
    public boolean supportsOrdinals() {
        return false;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException();
    }

    private record IgnoredSourceRowStrideReader(String fieldName, Reader reader) implements RowStrideReader {
        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            var ignoredSource = storedFields.storedFields().get(IgnoredSourceFieldMapper.NAME);
            if (ignoredSource == null) {
                return;
            }

            Map<String, List<IgnoredSourceFieldMapper.NameValue>> valuesForFieldAndParents = new HashMap<>();

            // Contains name of the field and all its parents
            Set<String> fieldNames = new HashSet<>() {
                {
                    add("_doc");
                }
            };

            var current = new StringBuilder();
            for (String part : fieldName.split("\\.")) {
                current.append(part);
                fieldNames.add(current.toString());
            }

            for (Object value : ignoredSource) {
                IgnoredSourceFieldMapper.NameValue nameValue = IgnoredSourceFieldMapper.decode(value);
                if (fieldNames.contains(nameValue.name())) {
                    valuesForFieldAndParents.computeIfAbsent(nameValue.name(), k -> new ArrayList<>()).add(nameValue);
                }
            }

            // TODO figure out how to handle XContentDataHelper#voidValue()

            if (readFromFieldValue(valuesForFieldAndParents.get(fieldName), builder)) {
                return;
            }
            if (readFromParentValue(valuesForFieldAndParents, builder)) {
                return;
            }

            builder.appendNull();
        }

        private boolean readFromFieldValue(List<IgnoredSourceFieldMapper.NameValue> nameValues, Builder builder) throws IOException {
            if (nameValues == null || nameValues.isEmpty()) {
                return false;
            }

            // TODO this is not working properly
            if (nameValues.size() > 1) {
                builder.beginPositionEntry();
            }

            for (var nameValue : nameValues) {
                // Leaf field is stored directly (not as a part of a parent object), let's try to decode it.
                Optional<Object> singleValue = XContentDataHelper.decode(nameValue.value());
                if (singleValue.isPresent()) {
                    reader.readValue(singleValue.get(), builder);
                    continue;
                }

                // We have a value for this field but it's an array or an object
                var type = XContentDataHelper.decodeType(nameValue.value());
                assert type.isPresent();

                try (
                    XContentParser parser = type.get()
                        .xContent()
                        .createParser(
                            XContentParserConfiguration.EMPTY,
                            nameValue.value().bytes,
                            nameValue.value().offset + 1,
                            nameValue.value().length - 1
                        )
                ) {
                    parser.nextToken();
                    reader.parse(parser, builder);
                }
            }

            if (nameValues.size() > 1) {
                builder.endPositionEntry();
            }

            return true;
        }

        private boolean readFromParentValue(Map<String, List<IgnoredSourceFieldMapper.NameValue>> valuesForFieldAndParents, Builder builder)
            throws IOException {
            if (valuesForFieldAndParents.isEmpty()) {
                return false;
            }

            // If a parent object is stored at a particular level its children won't be stored.
            // So we should only ever have one parent here.
            assert valuesForFieldAndParents.size() == 1 : "_ignored_source field contains multiple levels of the same object";
            var parentValues = valuesForFieldAndParents.values().iterator().next();

            for (var nameValue : parentValues) {
                parseFieldFromParent(nameValue, builder);
            }

            return true;
        }

        private void parseFieldFromParent(IgnoredSourceFieldMapper.NameValue nameValue, Builder builder) throws IOException {
            var type = XContentDataHelper.decodeType(nameValue.value());
            assert type.isPresent();

            String nameAtThisLevel = fieldName.substring(nameValue.name().length() + 1);
            var filterParserConfig = XContentParserConfiguration.EMPTY.withFiltering(null, Set.of(nameAtThisLevel), Set.of(), true);
            try (
                XContentParser parser = type.get()
                    .xContent()
                    .createParser(filterParserConfig, nameValue.value().bytes, nameValue.value().offset + 1, nameValue.value().length - 1)
            ) {
                parser.nextToken();
                var fieldNameInParser = new StringBuilder(nameValue.name());
                while (true) {
                    if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                        fieldNameInParser.append('.').append(parser.currentName());
                        if (fieldNameInParser.toString().equals(fieldName)) {
                            parser.nextToken();
                            break;
                        }
                    }
                    parser.nextToken();
                }

                reader.parse(parser, builder);
            }
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return false;
        }
    }

    public interface Reader {
        /**
         * Converts a raw stored value for this field to a value in a format suitable for block loader and appends it to block.
         * @param value raw decoded value from _ignored_source field (synthetic _source value)
         */
        void readValue(Object value, Builder builder);

        /**
         * Parses one or more complex values using a provided parser and appends results to a block.
         * @param parser parser of a value from _ignored_source field (synthetic _source value)
         */
        void parse(XContentParser parser, Builder builder) throws IOException;
    }
}
