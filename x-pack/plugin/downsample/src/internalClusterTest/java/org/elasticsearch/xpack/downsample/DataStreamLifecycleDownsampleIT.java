/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle.Downsampling;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.datastreams.lifecycle.action.PutDataStreamLifecycleAction;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DataStreamLifecycleDownsampleIT extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(DataStreamLifecycleDownsampleIT.class);
    public static final String FIELD_TIMESTAMP = "@timestamp";
    public static final String FIELD_DIMENSION_1 = "dimension_kw";
    public static final String FIELD_DIMENSION_2 = "dimension_long";
    public static final String FIELD_METRIC_COUNTER = "counter";
    public static final int DOC_COUNT = 1000;
    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, LocalStateCompositeXPackPlugin.class, Downsample.class, AggregateMetricMapperPlugin.class);
    }

    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");
        return settings.build();
    }

    @TestLogging(value = "org.elasticsearch.datastreams.lifecycle:TRACE", reason = "debugging")
    public void testDownsampling() throws Exception {
        String dataStreamName = "metrics-foo";

        DataStreamLifecycle lifecycle = DataStreamLifecycle.newBuilder()
            .downsampling(
                new Downsampling(
                    List.of(
                        new Downsampling.Round(TimeValue.timeValueMillis(0), new DownsampleConfig(new DateHistogramInterval("1s"))),
                        new Downsampling.Round(TimeValue.timeValueSeconds(10), new DownsampleConfig(new DateHistogramInterval("10s")))
                    )
                )
            )
            .build();

        Settings.Builder settings = indexSettings(1, 0).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_1));

        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties");
        mapping.startObject(FIELD_TIMESTAMP).field("type", "date").endObject();

        mapping.startObject(FIELD_DIMENSION_1).field("type", "keyword").field("time_series_dimension", true).endObject();
        mapping.startObject(FIELD_DIMENSION_2).field("type", "long").field("time_series_dimension", true).endObject();

        mapping.startObject(FIELD_METRIC_COUNTER)
            .field("type", "double") /* numeric label indexed as a metric */
            .field("time_series_metric", "counter")
            .endObject();

        mapping.endObject().endObject().endObject();

        putComposableIndexTemplate(
            "id1",
            CompressedXContent.fromJSON(Strings.toString(mapping)),
            List.of("metrics-foo*"),
            settings.build(),
            null,
            lifecycle
        );

        indexDocuments(dataStreamName);

        client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null)).actionGet();

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
            List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();

            assertThat(backingIndices.size(), is(2));
            // first downsampling
            assertThat(backingIndices.get(0).getName(), is("downsample-1s-.ds-metrics-foo-2023.08.17-000001"));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
            List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();

            assertThat(backingIndices.size(), is(2));
            String writeIndex = backingIndices.get(1).getName();
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
            assertThat(backingIndices.get(0).getName(), is("downsample-10s-.ds-metrics-foo-2023.08.17-000001"));
        }, 30, TimeUnit.SECONDS);
    }

    private void indexDocuments(String dataStreamName) {
        final Supplier<XContentBuilder> sourceSupplier = () -> {
            final String ts = randomDateForInterval(new DateHistogramInterval("1s"), System.currentTimeMillis());
            double counterValue = DATE_FORMATTER.parseMillis(ts);
            final List<String> dimensionValues = new ArrayList<>(5);
            for (int j = 0; j < randomIntBetween(1, 5); j++) {
                dimensionValues.add(randomAlphaOfLength(6));
            }
            try {
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field(FIELD_TIMESTAMP, ts)
                    .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                    .field(FIELD_DIMENSION_2, randomIntBetween(1, 10))
                    .field(FIELD_METRIC_COUNTER, counterValue)
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        bulkIndex(dataStreamName, sourceSupplier, DOC_COUNT);
    }

    private String randomDateForInterval(final DateHistogramInterval interval, final long startTime) {
        long endTime = startTime + 10 * interval.estimateMillis();
        return randomDateForRange(startTime, endTime);
    }

    private String randomDateForRange(long start, long end) {
        return DATE_FORMATTER.formatMillis(randomLongBetween(start, end));
    }

    private int bulkIndex(String dataStreamName, Supplier<XContentBuilder> docSourceSupplier, int docCount) {
        BulkRequestBuilder bulkRequestBuilder = internalCluster().client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docCount; i++) {
            IndexRequest indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
            XContentBuilder source = docSourceSupplier.get();
            indexRequest.source(source);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        int duplicates = 0;
        for (BulkItemResponse response : bulkResponse.getItems()) {
            if (response.isFailed()) {
                if (response.getFailure().getCause() instanceof VersionConflictEngineException) {
                    // A duplicate event was created by random generator. We should not fail for this
                    // reason.
                    logger.debug("-> failed to insert a duplicate: [{}]", response.getFailureMessage());
                    duplicates++;
                } else {
                    fail("Failed to index data: " + bulkResponse.buildFailureMessage());
                }
            }
        }
        int docsIndexed = docCount - duplicates;
        logger.info("-> Indexed [{}] documents. Dropped [{}] duplicates.", docsIndexed, duplicates);
        return docsIndexed;
    }

    static void putComposableIndexTemplate(
        String id,
        @Nullable CompressedXContent mappings,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable Map<String, Object> metadata,
        @Nullable DataStreamLifecycle lifecycle
    ) throws IOException {
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            new ComposableIndexTemplate(
                patterns,
                new Template(settings, mappings == null ? null : mappings, null, lifecycle),
                null,
                null,
                null,
                metadata,
                new ComposableIndexTemplate.DataStreamTemplate(),
                null
            )
        );
        client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();
    }

    static void updateLifecycle(String dataStreamName, TimeValue dataRetention) {
        PutDataStreamLifecycleAction.Request putDataLifecycleRequest = new PutDataStreamLifecycleAction.Request(
            new String[] { dataStreamName },
            dataRetention
        );
        AcknowledgedResponse putDataLifecycleResponse = client().execute(PutDataStreamLifecycleAction.INSTANCE, putDataLifecycleRequest)
            .actionGet();
        assertThat(putDataLifecycleResponse.isAcknowledged(), equalTo(true));
    }
}
