/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.settings.relevance;

import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.AbstractScriptScoreBoost;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.FunctionalBoost;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.ProximityBoost;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.ValueBoost;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RelevanceSettingsServiceTests extends ESSingleNodeTestCase {

    private RelevanceSettingsService service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        service = getInstanceFromNode(RelevanceSettingsService.class);
    }

    public void testParseFields() throws Exception {
        Map<String, Object> rawSettings = Map.of("query_configuration", Map.of("fields", List.of("title^3", "description^2")));
        RelevanceSettings settings = service.parseSettings(rawSettings);
        Map<String, Float> expected = Map.of("title", 3f, "description", 2f);
        assertEquals(expected, settings.getQueryConfiguration().getFieldsAndBoosts());
    }

    public void testParseValueBoost() throws Exception {
        Map<String, Object> rawSettings = Map.of(
            "query_configuration",
            Map.of(
                "fields",
                List.of("title", "description"),
                "boosts",
                Map.of("world_heritage_site", List.of(Map.of("type", "value", "operation", "multiply", "factor", "10", "value", "true")))
            )
        );
        RelevanceSettings settings = service.parseSettings(rawSettings);
        Map<String, List<AbstractScriptScoreBoost>> actual = settings.getQueryConfiguration().getScriptScores();

        Map<String, List<AbstractScriptScoreBoost>> expected = Collections.singletonMap(
            "world_heritage_site",
            Collections.singletonList(new ValueBoost("true", "multiply", 10f))
        );
        assertEquals(expected, actual);
    }

    public void testParseFunctionalBoost() throws Exception {
        Map<String, Object> rawSettings = Map.of(
            "query_configuration",
            Map.of(
                "fields",
                List.of("title", "description"),
                "boosts",
                Map.of("visitors", List.of(Map.of("type", "functional", "operation", "add", "factor", 5, "function", "linear")))
            )
        );
        RelevanceSettings settings = service.parseSettings(rawSettings);
        Map<String, List<AbstractScriptScoreBoost>> actual = settings.getQueryConfiguration().getScriptScores();

        Map<String, List<AbstractScriptScoreBoost>> expected = Collections.singletonMap(
            "visitors",
            Collections.singletonList(new FunctionalBoost("linear", "add", 5f))
        );
        assertEquals(expected, actual);
    }

    public void testParseProximityBoost() throws Exception {
        Map<String, Object> rawSettings = Map.of(
            "query_configuration",
            Map.of(
                "fields",
                List.of("title", "description"),
                "boosts",
                Map.of("location", List.of(Map.of("type", "proximity", "center", "25.32, -80.93", "factor", 5, "function", "gaussian")))
            )
        );
        RelevanceSettings settings = service.parseSettings(rawSettings);
        Map<String, List<AbstractScriptScoreBoost>> actual = settings.getQueryConfiguration().getScriptScores();

        Map<String, List<AbstractScriptScoreBoost>> expected = Collections.singletonMap(
            "location",
            Collections.singletonList(new ProximityBoost("25.32, -80.93", "gaussian", 5f))
        );
        assertEquals(expected, actual);
    }

    public void testParseMultipleBoosts() throws Exception {
        Map<String, Object> rawSettings = Map.of(
            "query_configuration",
            Map.of(
                "fields",
                List.of("title", "description"),
                "boosts",
                Map.of(
                    "location",
                    List.of(Map.of("type", "proximity", "center", "25.32, -80.93", "factor", 5, "function", "gaussian")),
                    "visitors",
                    List.of(Map.of("type", "functional", "operation", "add", "factor", 5, "function", "linear")),
                    "world_heritage_site",
                    List.of(Map.of("type", "value", "operation", "multiply", "factor", "10", "value", "true"))
                )
            )
        );
        RelevanceSettings settings = service.parseSettings(rawSettings);
        Map<String, List<AbstractScriptScoreBoost>> actual = settings.getQueryConfiguration().getScriptScores();

        Map<String, List<AbstractScriptScoreBoost>> expected = Map.of(
            "location",
            Collections.singletonList(new ProximityBoost("25.32, -80.93", "gaussian", 5f)),
            "visitors",
            Collections.singletonList(new FunctionalBoost("linear", "add", 5f)),
            "world_heritage_site",
            Collections.singletonList(new ValueBoost("true", "multiply", 10f))
        );
        assertEquals(expected, actual);
    }

    public void testParseMultipleBoostsSameField() throws Exception {
        Map<String, Object> rawSettings = Map.of(
            "query_configuration",
            Map.of(
                "fields",
                List.of("title", "description"),
                "boosts",
                Map.of(
                    "visitors",
                    List.of(
                        Map.of("type", "functional", "operation", "add", "factor", 5, "function", "linear"),
                        Map.of("type", "functional", "operation", "multiply", "factor", 3, "function", "logarithmic")
                    )
                )
            )
        );
        RelevanceSettings settings = service.parseSettings(rawSettings);
        Map<String, List<AbstractScriptScoreBoost>> actual = settings.getQueryConfiguration().getScriptScores();

        Map<String, List<AbstractScriptScoreBoost>> expected = Map.of(
            "visitors",
            List.of(new FunctionalBoost("linear", "add", 5f), new FunctionalBoost("logarithmic", "multiply", 3f))
        );
        assertEquals(expected, actual);
    }
}
