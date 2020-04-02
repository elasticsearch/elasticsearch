/t a*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;

public class RegressionConfigTests extends AbstractBWCSerializationTestCase<RegressionConfig> {
    private boolean lenient;

    public static RegressionConfig randomRegressionConfig() {
        return new RegressionConfig(randomBoolean() ? null : randomAlphaOfLength(10));
    }

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected RegressionConfig createTestInstance() {
        return randomRegressionConfig();
    }

    @Override
    protected Writeable.Reader<RegressionConfig> instanceReader() {
        return RegressionConfig::new;
    }

    @Override
    protected RegressionConfig doParseInstance(XContentParser parser) throws IOException {
        return lenient ? RegressionConfig.fromXContentLenient(parser) : RegressionConfig.fromXContentStrict(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected RegressionConfig mutateInstanceForVersion(RegressionConfig instance, Version version) {
        return instance;
    }
}
