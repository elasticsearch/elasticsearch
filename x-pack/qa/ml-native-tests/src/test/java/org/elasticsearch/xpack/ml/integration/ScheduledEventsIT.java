/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.junit.After;
import org.junit.Ignore;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@Ignore
public class ScheduledEventsIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanUpTest() {
        cleanUp();
    }

    public void testScheduledEvents() throws IOException {

        TimeValue bucketSpan = TimeValue.timeValueMinutes(30);
        Job.Builder job = createJob("scheduled-events", bucketSpan);
        String calendarId = "test-calendar";
        putCalendar(calendarId, Collections.singletonList(job.getId()), "testScheduledEvents calendar");

        long startTime = 1514764800000L;

        List<ScheduledEvent> events = new ArrayList<>();
        long firstEventStartTime = 1514937600000L;
        long firstEventEndTime = firstEventStartTime + 2 * 60 * 60 * 1000;
        events.add(new ScheduledEvent.Builder().description("1st event (2hr)")
                .startTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(firstEventStartTime), ZoneOffset.UTC))
                .endTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(firstEventEndTime), ZoneOffset.UTC))
                .calendarId(calendarId).build());
        // add 10 min event smaller than the bucket
        long secondEventStartTime = 1515067200000L;
        long secondEventEndTime = secondEventStartTime + 10 * 60 * 1000;
        events.add(new ScheduledEvent.Builder().description("2nd event with period smaller than bucketspan")
                .startTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(secondEventStartTime), ZoneOffset.UTC))
                .endTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(secondEventEndTime), ZoneOffset.UTC))
                .calendarId(calendarId).build());
        long thirdEventStartTime = 1515088800000L;
        long thirdEventEndTime = thirdEventStartTime + 3 * 60 * 60 * 1000;
        events.add(new ScheduledEvent.Builder().description("3rd event 3hr")
                .startTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(thirdEventStartTime), ZoneOffset.UTC))
                .endTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(thirdEventEndTime), ZoneOffset.UTC))
                .calendarId(calendarId).build());

        postScheduledEvents(calendarId, events);

        // Run 6 days of data
        runJob(job, startTime, bucketSpan, 2 * 24 * 6);

        // Check tags on the buckets during the first event
        GetBucketsAction.Request getBucketsRequest = new GetBucketsAction.Request(job.getId());
        getBucketsRequest.setStart(Long.toString(firstEventStartTime));
        getBucketsRequest.setEnd(Long.toString(firstEventEndTime));
        List<Bucket> buckets = getBuckets(getBucketsRequest);
        for (Bucket bucket : buckets) {
            assertEquals(1, bucket.getScheduledEvents().size());
            assertEquals("1st event (2hr)", bucket.getScheduledEvents().get(0));
            assertEquals(0.0, bucket.getAnomalyScore(), 0.00001);
        }

        // Following buckets have 0 events
        getBucketsRequest = new GetBucketsAction.Request(job.getId());
        getBucketsRequest.setStart(Long.toString(firstEventEndTime));
        getBucketsRequest.setEnd(Long.toString(secondEventStartTime));
        buckets = getBuckets(getBucketsRequest);
        for (Bucket bucket : buckets) {
            assertEquals(0, bucket.getScheduledEvents().size());
        }

        // The second event bucket
        getBucketsRequest.setStart(Long.toString(secondEventStartTime));
        getBucketsRequest.setEnd(Long.toString(secondEventEndTime));
        buckets = getBuckets(getBucketsRequest);
        assertEquals(1, buckets.size());
        for (Bucket bucket : buckets) {
            assertEquals(1, bucket.getScheduledEvents().size());
            assertEquals("2nd event with period smaller than bucketspan", bucket.getScheduledEvents().get(0));
            assertEquals(0.0, bucket.getAnomalyScore(), 0.00001);
        }

        // Following buckets have 0 events
        getBucketsRequest.setStart(Long.toString(secondEventEndTime));
        getBucketsRequest.setEnd(Long.toString(thirdEventStartTime));
        buckets = getBuckets(getBucketsRequest);
        for (Bucket bucket : buckets) {
            assertEquals(0, bucket.getScheduledEvents().size());
        }

        // The 3rd event buckets
        getBucketsRequest.setStart(Long.toString(thirdEventStartTime));
        getBucketsRequest.setEnd(Long.toString(thirdEventEndTime));
        buckets = getBuckets(getBucketsRequest);
        for (Bucket bucket : buckets) {
            assertEquals(1, bucket.getScheduledEvents().size());
            assertEquals("3rd event 3hr", bucket.getScheduledEvents().get(0));
            assertEquals(0.0, bucket.getAnomalyScore(), 0.00001);
        }

        // Following buckets have 0 events
        getBucketsRequest.setStart(Long.toString(thirdEventEndTime));
        getBucketsRequest.setEnd(null);
        buckets = getBuckets(getBucketsRequest);
        for (Bucket bucket : buckets) {
            assertEquals(0, bucket.getScheduledEvents().size());
        }

        // It is unlikely any anomaly records have been created but
        // ensure there are non present anyway
        GetRecordsAction.Request getRecordsRequest = new GetRecordsAction.Request(job.getId());
        getRecordsRequest.setStart(Long.toString(firstEventStartTime));
        getRecordsRequest.setEnd(Long.toString(firstEventEndTime));
        List<AnomalyRecord> records = getRecords(getRecordsRequest);
        assertThat(records, is(empty()));

        getRecordsRequest.setStart(Long.toString(secondEventStartTime));
        getRecordsRequest.setEnd(Long.toString(secondEventEndTime));
        records = getRecords(getRecordsRequest);
        assertThat(records, is(empty()));

        getRecordsRequest.setStart(Long.toString(thirdEventStartTime));
        getRecordsRequest.setEnd(Long.toString(thirdEventEndTime));
        records = getRecords(getRecordsRequest);
        assertThat(records, is(empty()));
    }

    public void testScheduledEventWithInterimResults() throws IOException {
        TimeValue bucketSpan = TimeValue.timeValueMinutes(30);
        Job.Builder job = createJob("scheduled-events-interim-results", bucketSpan);
        String calendarId = "test-calendar";
        putCalendar(calendarId, Collections.singletonList(job.getId()), "testScheduledEventWithInterimResults calendar");

        long startTime = 1514764800000L;

        List<ScheduledEvent> events = new ArrayList<>();
        // The event starts 10 buckets in and lasts for 2
        int bucketCount = 10;
        long firstEventStartTime = startTime + bucketSpan.millis() * bucketCount;
        long firstEventEndTime = firstEventStartTime + bucketSpan.millis() * 2;
        events.add(new ScheduledEvent.Builder().description("1st event 2hr")
                .startTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(firstEventStartTime), ZoneOffset.UTC))
                .endTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(firstEventEndTime), ZoneOffset.UTC))
                .calendarId(calendarId).build());
        postScheduledEvents(calendarId, events);


        openJob(job.getId());
        // write data up to and including the event
        postData(job.getId(), generateData(startTime, bucketSpan, bucketCount + 1, bucketIndex -> randomIntBetween(100, 200))
                .stream().collect(Collectors.joining()));

        // flush the job and get the interim result during the event
        flushJob(job.getId(), true);

        GetBucketsAction.Request getBucketsRequest = new GetBucketsAction.Request(job.getId());
        getBucketsRequest.setStart(Long.toString(firstEventStartTime));
        List<Bucket> buckets = getBuckets(getBucketsRequest);
        assertEquals(1, buckets.size());
        assertTrue(buckets.get(0).isInterim());
        assertEquals(1, buckets.get(0).getScheduledEvents().size());
        assertEquals("1st event 2hr", buckets.get(0).getScheduledEvents().get(0));
        assertEquals(0.0, buckets.get(0).getAnomalyScore(), 0.00001);
    }

    /**
     * Test an open job picks up changes to scheduled events/calendars
     */
    public void testOnlineUpdate() throws Exception {
        TimeValue bucketSpan = TimeValue.timeValueMinutes(30);
        Job.Builder job = createJob("scheduled-events-online-update", bucketSpan);

        long startTime = 1514764800000L;
        final int bucketCount = 5;

        // Open the job
        openJob(job.getId());

        // write some buckets of data
        postData(job.getId(), generateData(startTime, bucketSpan, bucketCount, bucketIndex -> randomIntBetween(100, 200))
                .stream().collect(Collectors.joining()));

        // Now create a calendar and events for the job while it is open
        String calendarId = "test-calendar-online-update";
        putCalendar(calendarId, Collections.singletonList(job.getId()), "testOnlineUpdate calendar");

        List<ScheduledEvent> events = new ArrayList<>();
        long eventStartTime = startTime + (bucketCount + 1) * bucketSpan.millis();
        long eventEndTime = eventStartTime + (long)(1.5 * bucketSpan.millis());
        events.add(new ScheduledEvent.Builder().description("Some Event")
                .startTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(eventStartTime), ZoneOffset.UTC))
                .endTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(eventEndTime), ZoneOffset.UTC))
                .calendarId(calendarId).build());

        postScheduledEvents(calendarId, events);

        // Wait until the notification that the process was updated is indexed
        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(".ml-notifications")
                    .setSize(1)
                    .addSort("timestamp", SortOrder.DESC)
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.termQuery("job_id", job.getId()))
                            .filter(QueryBuilders.termQuery("level", "info"))
                    ).get();
            SearchHit[] hits = searchResponse.getHits().getHits();
            assertThat(hits.length, equalTo(1));
            assertThat(hits[0].getSourceAsMap().get("message"), equalTo("Updated calendars in running process"));
        });

        // write some more buckets of data that cover the scheduled event period
        postData(job.getId(), generateData(startTime + bucketCount * bucketSpan.millis(), bucketSpan, 5,
                bucketIndex -> randomIntBetween(100, 200))
                .stream().collect(Collectors.joining()));
        // and close
        closeJob(job.getId());

        GetBucketsAction.Request getBucketsRequest = new GetBucketsAction.Request(job.getId());
        List<Bucket> buckets = getBuckets(getBucketsRequest);

        // the first buckets have no events
        for (int i=0; i<=bucketCount; i++) {
            assertEquals(0, buckets.get(i).getScheduledEvents().size());
        }
        // 7th and 8th buckets have the event
        assertEquals(1, buckets.get(6).getScheduledEvents().size());
        assertEquals("Some Event", buckets.get(6).getScheduledEvents().get(0));
        assertEquals(1, buckets.get(7).getScheduledEvents().size());
        assertEquals("Some Event", buckets.get(7).getScheduledEvents().get(0));
        assertEquals(0, buckets.get(8).getScheduledEvents().size());
    }

    private Job.Builder createJob(String jobId, TimeValue bucketSpan) {
        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        Job.Builder job = new Job.Builder(jobId);
        job.setAnalysisConfig(analysisConfig);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        job.setDataDescription(dataDescription);
        putJob(job);
        // register for clean up
        registerJob(job);

        return job;
    }

    private void runJob(Job.Builder job, long startTime, TimeValue bucketSpan, int bucketCount) throws IOException {
        openJob(job.getId());
        postData(job.getId(), generateData(startTime, bucketSpan, bucketCount, bucketIndex -> randomIntBetween(100, 200))
                .stream().collect(Collectors.joining()));
        closeJob(job.getId());
    }
}
