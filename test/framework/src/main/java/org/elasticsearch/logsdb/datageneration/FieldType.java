/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.logsdb.datageneration.datasource.DataSource;
import org.elasticsearch.logsdb.datageneration.fields.leaf.ByteFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.DoubleFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.FloatFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.HalfFloatFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.IntegerFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.KeywordFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.LongFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.ScaledFloatFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.ShortFieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.leaf.UnsignedLongFieldDataGenerator;

/**
 * Lists all leaf field types that are supported for data generation.
 */
public enum FieldType {
    KEYWORD,
    LONG,
    UNSIGNED_LONG,
    INTEGER,
    SHORT,
    BYTE,
    DOUBLE,
    FLOAT,
    HALF_FLOAT,
    SCALED_FLOAT;

    public FieldDataGenerator generator(String fieldName, DataSource dataSource) {
        return switch (this) {
            case KEYWORD -> new KeywordFieldDataGenerator(fieldName, dataSource);
            case LONG -> new LongFieldDataGenerator(fieldName, dataSource);
            case UNSIGNED_LONG -> new UnsignedLongFieldDataGenerator(fieldName, dataSource);
            case INTEGER -> new IntegerFieldDataGenerator(fieldName, dataSource);
            case SHORT -> new ShortFieldDataGenerator(fieldName, dataSource);
            case BYTE -> new ByteFieldDataGenerator(fieldName, dataSource);
            case DOUBLE -> new DoubleFieldDataGenerator(fieldName, dataSource);
            case FLOAT -> new FloatFieldDataGenerator(fieldName, dataSource);
            case HALF_FLOAT -> new HalfFloatFieldDataGenerator(fieldName, dataSource);
            case SCALED_FLOAT -> new ScaledFloatFieldDataGenerator(fieldName, dataSource);
        };
    }
}
