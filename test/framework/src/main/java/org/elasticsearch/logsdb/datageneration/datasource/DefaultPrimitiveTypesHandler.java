/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.datasource;

import org.elasticsearch.test.ESTestCase;

import java.math.BigInteger;

public class DefaultPrimitiveTypesHandler implements DataSourceHandler {
    @Override
    public DataSourceResponse handle(DataSourceRequest.LongGenerator request) {
        return new DataSourceResponse.LongGenerator(ESTestCase::randomLong);
    }

    @Override
    public DataSourceResponse handle(DataSourceRequest.UnsignedLongGenerator request) {
        return new DataSourceResponse.UnsignedLongGenerator(() -> new BigInteger(64, ESTestCase.random()).toString());
    }

    @Override
    public DataSourceResponse handle(DataSourceRequest.IntegerGenerator request) {
        return new DataSourceResponse.IntegerGenerator(ESTestCase::randomInt);
    }

    @Override
    public DataSourceResponse handle(DataSourceRequest.ShortGenerator request) {
        return new DataSourceResponse.ShortGenerator(ESTestCase::randomShort);
    }

    @Override
    public DataSourceResponse handle(DataSourceRequest.ByteGenerator request) {
        return new DataSourceResponse.ByteGenerator(ESTestCase::randomByte);
    }

    @Override
    public DataSourceResponse handle(DataSourceRequest.DoubleGenerator request) {
        return new DataSourceResponse.DoubleGenerator(ESTestCase::randomDouble);
    }

    @Override
    public DataSourceResponse handle(DataSourceRequest.DoubleInRangeGenerator request) {
        return new DataSourceResponse.DoubleInRangeGenerator(
            () -> ESTestCase.randomDoubleBetween(request.minExclusive(), request.maxExclusive(), false)
        );
    }

    @Override
    public DataSourceResponse handle(DataSourceRequest.FloatGenerator request) {
        return new DataSourceResponse.FloatGenerator(ESTestCase::randomFloat);
    }

    @Override
    public DataSourceResponse handle(DataSourceRequest.HalfFloatGenerator request) {
        return new DataSourceResponse.HalfFloatGenerator(() -> ESTestCase.randomFloat());
    }

    @Override
    public DataSourceResponse handle(DataSourceRequest.StringGenerator request) {
        return new DataSourceResponse.StringGenerator(() -> ESTestCase.randomAlphaOfLengthBetween(0, 50));
    }
}
