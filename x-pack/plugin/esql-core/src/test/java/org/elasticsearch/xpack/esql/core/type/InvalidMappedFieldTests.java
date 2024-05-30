/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

public class InvalidMappedFieldTests extends AbstractEsFieldTypeTests<InvalidMappedField> {
    static InvalidMappedField randomInvalidMappedField(int depth) {
        String name = randomAlphaOfLength(4);
        String errorMessage = randomAlphaOfLengthBetween(1, 100);
        Map<String, EsField> properties = randomProperties(depth);
        return new InvalidMappedField(name, errorMessage, properties);
    }

    @Override
    protected InvalidMappedField createTestInstance() {
        return randomInvalidMappedField(0);
    }

    @Override
    protected InvalidMappedField mutate(InvalidMappedField instance) throws IOException {
        String name = instance.getName();
        String errorMessage = instance.errorMessage();
        Map<String, EsField> properties = instance.getProperties();
        switch (between(0, 2)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> errorMessage = randomValueOtherThan(errorMessage, () -> randomAlphaOfLengthBetween(1, 100));
            case 2 -> properties = randomValueOtherThan(properties, () -> randomProperties(0));
            default -> throw new IllegalArgumentException();
        }
        return new InvalidMappedField(name, errorMessage, properties);
    }
}
