/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.stats;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.stats.ForecastStats;
import org.elasticsearch.xpack.core.ml.stats.ForecastStats.Fields;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ForecastStatsTests extends AbstractWireSerializingTestCase<ForecastStats> {

    public void testEmpty() throws IOException {
        ForecastStats forecastStats = new ForecastStats();

        XContentBuilder builder = JsonXContent.contentBuilder();
        forecastStats.toXContent(builder, ToXContent.EMPTY_PARAMS);

        XContentParser parser = createParser(builder);
        Map<String, Object> properties = parser.map();
        assertTrue(properties.containsKey(Fields.TOTAL));
        assertFalse(properties.containsKey(Fields.MEMORY));
        assertFalse(properties.containsKey(Fields.RECORDS));
        assertFalse(properties.containsKey(Fields.RUNTIME));
        assertFalse(properties.containsKey(Fields.JOBS));
        assertFalse(properties.containsKey(Fields.STATUSES));
    }

    public void testMerge() {
        StatsAccumulator memoryStats = new StatsAccumulator();
        memoryStats.add(1000);
        memoryStats.add(45000);
        memoryStats.add(2300);

        StatsAccumulator recordStats = new StatsAccumulator();
        recordStats.add(10);
        recordStats.add(0);
        recordStats.add(20);

        StatsAccumulator runtimeStats = new StatsAccumulator();
        runtimeStats.add(0);
        runtimeStats.add(0);
        runtimeStats.add(10);

        CountAccumulator statusStats = new CountAccumulator();
        statusStats.add("finished", 2l);
        statusStats.add("failed", 5l);

        ForecastStats forecastStats = new ForecastStats(3, memoryStats, recordStats, runtimeStats, statusStats);

        StatsAccumulator memoryStats2 = new StatsAccumulator();
        memoryStats2.add(10);
        memoryStats2.add(30);

        StatsAccumulator recordStats2 = new StatsAccumulator();
        recordStats2.add(10);
        recordStats2.add(0);

        StatsAccumulator runtimeStats2 = new StatsAccumulator();
        runtimeStats2.add(96);
        runtimeStats2.add(0);

        CountAccumulator statusStats2 = new CountAccumulator();
        statusStats2.add("finished", 2l);
        statusStats2.add("scheduled", 1l);

        ForecastStats forecastStats2 = new ForecastStats(2, memoryStats2, recordStats2, runtimeStats2, statusStats2);

        forecastStats.merge(forecastStats2);

        Map<String, Object> mergedStats = forecastStats.asMap();

        assertEquals(2l, mergedStats.get(Fields.JOBS));
        assertEquals(5l, mergedStats.get(Fields.TOTAL));

        @SuppressWarnings("unchecked")
        Map<String, Double> mergedMemoryStats = (Map<String, Double>) mergedStats.get(Fields.MEMORY);

        assertTrue(mergedMemoryStats != null);
        assertThat(mergedMemoryStats.get(StatsAccumulator.Fields.AVG), equalTo(9668.0));
        assertThat(mergedMemoryStats.get(StatsAccumulator.Fields.MAX), equalTo(45000.0));
        assertThat(mergedMemoryStats.get(StatsAccumulator.Fields.MIN), equalTo(10.0));

        @SuppressWarnings("unchecked")
        Map<String, Double> mergedRecordStats = (Map<String, Double>) mergedStats.get(Fields.RECORDS);

        assertTrue(mergedRecordStats != null);
        assertThat(mergedRecordStats.get(StatsAccumulator.Fields.AVG), equalTo(8.0));
        assertThat(mergedRecordStats.get(StatsAccumulator.Fields.MAX), equalTo(20.0));
        assertThat(mergedRecordStats.get(StatsAccumulator.Fields.MIN), equalTo(0.0));

        @SuppressWarnings("unchecked")
        Map<String, Double> mergedRuntimeStats = (Map<String, Double>) mergedStats.get(Fields.RUNTIME);

        assertTrue(mergedRuntimeStats != null);
        assertThat(mergedRuntimeStats.get(StatsAccumulator.Fields.AVG), equalTo(21.2));
        assertThat(mergedRuntimeStats.get(StatsAccumulator.Fields.MAX), equalTo(96.0));
        assertThat(mergedRuntimeStats.get(StatsAccumulator.Fields.MIN), equalTo(0.0));

        @SuppressWarnings("unchecked")
        Map<String, Long> mergedCountStats = (Map<String, Long>) mergedStats.get(Fields.STATUSES);

        assertTrue(mergedCountStats != null);
        assertEquals(3, mergedCountStats.size());
        assertEquals(4, mergedCountStats.get("finished").longValue());
        assertEquals(5, mergedCountStats.get("failed").longValue());
        assertEquals(1, mergedCountStats.get("scheduled").longValue());
    }

    public void testUniqueCountOfJobs() {
        ForecastStats forecastStats = createForecastStats(5, 10);
        ForecastStats forecastStats2 = createForecastStats(2, 8);
        ForecastStats forecastStats3 = createForecastStats(0, 0);
        ForecastStats forecastStats4 = createForecastStats(0, 0);
        ForecastStats forecastStats5 = createForecastStats(1, 12);

        forecastStats.merge(forecastStats2);
        forecastStats.merge(forecastStats3);
        forecastStats.merge(forecastStats4);
        forecastStats.merge(forecastStats5);

        assertEquals(3l, forecastStats.asMap().get(Fields.JOBS));
    }

    @Override
    public ForecastStats createTestInstance() {
        return createForecastStats(1, 22);
    }

    @Override
    protected Reader<ForecastStats> instanceReader() {
        return ForecastStats::new;
    }

    private ForecastStats createForecastStats(long minTotal, long maxTotal) {
        ForecastStats forecastStats = new ForecastStats(randomLongBetween(minTotal, maxTotal), createStatsAccumulator(),
                createStatsAccumulator(), createStatsAccumulator(), createCountAccumulator());

        return forecastStats;
    }

    private StatsAccumulator createStatsAccumulator() {
        return new StatsAccumulatorTests().createTestInstance();
    }

    private CountAccumulator createCountAccumulator() {
        return new CountAccumulatorTests().createTestInstance();

    }
}
