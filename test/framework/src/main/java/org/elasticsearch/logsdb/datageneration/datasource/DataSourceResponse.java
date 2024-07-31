/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.datasource;

import org.elasticsearch.logsdb.datageneration.FieldType;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public interface DataSourceResponse {
    /**
     * Internal signal that a handler does not process such requests.
     */
    record NotMatched() implements DataSourceResponse {}

    record LongGenerator(Supplier<Long> generator) implements DataSourceResponse {}

    record UnsignedLongGenerator(Supplier<Object> generator) implements DataSourceResponse {}

    record IntegerGenerator(Supplier<Integer> generator) implements DataSourceResponse {}

    record ShortGenerator(Supplier<Short> generator) implements DataSourceResponse {}

    record ByteGenerator(Supplier<Byte> generator) implements DataSourceResponse {}

    record DoubleGenerator(Supplier<Double> generator) implements DataSourceResponse {}

    record DoubleInRangeGenerator(Supplier<Double> generator) implements DataSourceResponse {}

    record FloatGenerator(Supplier<Float> generator) implements DataSourceResponse {}

    record HalfFloatGenerator(Supplier<Float> generator) implements DataSourceResponse {}

    record StringGenerator(Supplier<String> generator) implements DataSourceResponse {}

    record NullWrapper(Function<Supplier<Object>, Supplier<Object>> wrapper) implements DataSourceResponse {}

    record ArrayWrapper(Function<Supplier<Object>, Supplier<Object>> wrapper) implements DataSourceResponse {}

    interface ChildFieldGenerator extends DataSourceResponse {
        int generateChildFieldCount();

        boolean generateNestedSubObject();

        boolean generateRegularSubObject();

        String generateFieldName();
    }

    record FieldTypeGenerator(Supplier<FieldType> generator) implements DataSourceResponse {}

    record ObjectArrayGenerator(Supplier<Optional<Integer>> lengthGenerator) implements DataSourceResponse {}
}
