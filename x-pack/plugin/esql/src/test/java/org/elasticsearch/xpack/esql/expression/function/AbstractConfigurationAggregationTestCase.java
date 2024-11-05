/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlFeatures;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.SerializationTestUtils.assertSerialization;

public abstract class AbstractConfigurationAggregationTestCase extends AbstractAggregationTestCase {
    protected abstract Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration);

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return buildWithConfiguration(source, args, EsqlTestUtils.TEST_CFG);
    }

    static Configuration randomConfiguration() {
        // TODO: Randomize the query and maybe the pragmas.
        return new Configuration(
            randomZone(),
            randomLocale(random()),
            randomBoolean() ? null : randomAlphaOfLength(randomInt(64)),
            randomBoolean() ? null : randomAlphaOfLength(randomInt(64)),
            QueryPragmas.EMPTY,
            EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            StringUtils.EMPTY,
            randomBoolean(),
            Map.of(),
            System.nanoTime(),
            new EsqlFeatures().getFeatures().stream().map(NodeFeature::id).collect(Collectors.toSet())
        );
    }

    public void testSerializationWithConfiguration() {
        Configuration config = randomConfiguration();
        Expression expr = buildWithConfiguration(testCase.getSource(), testCase.getDataAsFields(), config);

        assertSerialization(expr, config);

        Configuration differentConfig;
        do {
            differentConfig = randomConfiguration();
        } while (config.equals(differentConfig));

        Expression differentExpr = buildWithConfiguration(testCase.getSource(), testCase.getDataAsFields(), differentConfig);
        assertFalse(expr.equals(differentExpr));
    }
}
