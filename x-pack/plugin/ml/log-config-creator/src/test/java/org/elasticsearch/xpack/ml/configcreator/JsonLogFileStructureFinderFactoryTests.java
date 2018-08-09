/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

public class JsonLogFileStructureFinderFactoryTests extends LogConfigCreatorTestCase {

    private LogFileStructureFinderFactory factory = new JsonLogFileStructureFinderFactory();

    public void testCanCreateFromSampleGivenJson() {

        assertTrue(factory.canCreateFromSample(explanation, JSON_SAMPLE));
    }

    public void testCanCreateFromSampleGivenXml() {

        assertFalse(factory.canCreateFromSample(explanation, XML_SAMPLE));
    }

    public void testCanCreateFromSampleGivenCsv() {

        assertFalse(factory.canCreateFromSample(explanation, CSV_SAMPLE));
    }

    public void testCanCreateFromSampleGivenTsv() {

        assertFalse(factory.canCreateFromSample(explanation, TSV_SAMPLE));
    }

    public void testCanCreateFromSampleGivenSemiColonSeparatedValues() {

        assertFalse(factory.canCreateFromSample(explanation, SEMI_COLON_SEPARATED_VALUES_SAMPLE));
    }

    public void testCanCreateFromSampleGivenPipeSeparatedValues() {

        assertFalse(factory.canCreateFromSample(explanation, PIPE_SEPARATED_VALUES_SAMPLE));
    }

    public void testCanCreateFromSampleGivenText() {

        assertFalse(factory.canCreateFromSample(explanation, TEXT_SAMPLE));
    }
}
