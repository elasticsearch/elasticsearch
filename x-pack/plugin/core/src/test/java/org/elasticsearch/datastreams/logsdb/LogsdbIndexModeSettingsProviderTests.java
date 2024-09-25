/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.datastreams.logsdb;

import junit.framework.TestCase;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

public class LogsdbIndexModeSettingsProviderTests extends TestCase {

    public static final String DEFAULT_MAPPING = """
        {
            "_doc": {
                "properties": {
                    "@timestamp": {
                        "type": "date"
                    },
                    "message": {
                        "type": "keyword"
                    },
                    "host.name": {
                        "type": "keyword"
                    }
                }
            }
        }
        """;

    public void testLogsDbDisabled() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", false).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            "logs-apache-production",
            false,
            Metadata.EMPTY_METADATA,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, null);
    }

    public void testOnIndexCreation() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            "logs-apache-production",
            null,
            false,
            Metadata.EMPTY_METADATA,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, null);
    }

    public void testOnExplicitStandardIndex() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            "logs-apache-production",
            false,
            Metadata.EMPTY_METADATA,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.builder().put("index.mode", "standard").build(),
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, null);
    }

    public void testOnExplicitTimeSeriesIndex() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            "logs-apache-production",
            false,
            Metadata.EMPTY_METADATA,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.builder().put("index.mode", "time_series").build(),
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, null);
    }

    public void testNonLogsDataStream() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            "logs",
            false,
            Metadata.EMPTY_METADATA,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, null);
    }

    public void testWithoutLogsComponentTemplate() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            "logs-apache-production",
            false,
            buildMetadata(List.of("*"), List.of()),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, null);
    }

    public void testWithLogsComponentTemplate() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            "logs-apache-production",
            false,
            buildMetadata(List.of("*"), List.of("logs@settings")),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, "logsdb");
    }

    public void testWithMultipleComponentTemplates() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            "logs-apache-production",
            false,
            buildMetadata(List.of("*"), List.of("logs@settings", "logs@custom")),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, "logsdb");
    }

    public void testWithCustomComponentTemplatesOnly() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            "logs-apache-production",
            false,
            buildMetadata(List.of("*"), List.of("logs@custom", "custom-component-template")),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, null);
    }

    public void testNonMatchingTemplateIndexPattern() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", true).build()
        );

        final Settings additionalIndexSettings = provider.getAdditionalIndexSettings(
            null,
            "logs-apache-production",
            false,
            buildMetadata(List.of("standard-apache-production"), List.of("logs@settings")),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(additionalIndexSettings, null);
    }

    public void testBeforeAndAFterSettingUpdate() throws IOException {
        final LogsdbIndexModeSettingsProvider provider = new LogsdbIndexModeSettingsProvider(
            Settings.builder().put("cluster.logsdb.enabled", false).build()
        );

        final Settings beforeSettings = provider.getAdditionalIndexSettings(
            null,
            "logs-apache-production",
            false,
            buildMetadata(List.of("*"), List.of("logs@settings")),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(beforeSettings, null);

        provider.updateClusterIndexModeLogsdbEnabled(true);

        final Settings afterSettings = provider.getAdditionalIndexSettings(
            null,
            "logs-apache-production",
            false,
            buildMetadata(List.of("*"), List.of("logs@settings")),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(afterSettings, "logsdb");

        provider.updateClusterIndexModeLogsdbEnabled(false);

        final Settings laterSettings = provider.getAdditionalIndexSettings(
            null,
            "logs-apache-production",
            false,
            buildMetadata(List.of("*"), List.of("logs@settings")),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Settings.EMPTY,
            List.of(new CompressedXContent(DEFAULT_MAPPING))
        );

        assertIndexMode(laterSettings, null);
    }

    private static Metadata buildMetadata(
        final List<String> indexPatterns,
        final List<String> componentTemplates
    ) throws IOException {
        final Template template = new Template(Settings.EMPTY, new CompressedXContent(DEFAULT_MAPPING), null);
        final ComposableIndexTemplate composableTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(indexPatterns)
            .template(template)
            .componentTemplates(componentTemplates)
            .priority(1_000L)
            .version(1L)
            .build();
        return Metadata.builder()
            .putCustom(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(Map.of("composable", composableTemplate)))
            .build();
    }

    private void assertIndexMode(final Settings settings, final String expectedIndexMode) {
        assertEquals(expectedIndexMode, settings.get("index.mode"));
    }

}
