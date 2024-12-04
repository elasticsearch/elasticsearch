/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndexSettingProviderTests extends ESSingleNodeTestCase {

    public void testIndexCreation() throws Exception {
        Settings settings = Settings.builder().put("index.mapping.depth.limit", 10).build();
        var indexService = createIndex("my-index1", settings);
        assertFalse(indexService.getIndexSettings().getSettings().hasValue("index.refresh_interval"));
        assertEquals("10", indexService.getIndexSettings().getSettings().get("index.mapping.depth.limit"));

        INDEX_SETTING_PROVIDER1_ENABLED.set(true);
        indexService = createIndex("my-index2", settings);
        assertTrue(indexService.getIndexSettings().getSettings().hasValue("index.refresh_interval"));
        assertEquals("10", indexService.getIndexSettings().getSettings().get("index.mapping.depth.limit"));

        INDEX_SETTING_PROVIDER2_ENABLED.set(true);
        indexService = createIndex("my-index3", settings);
        assertTrue(indexService.getIndexSettings().getSettings().hasValue("index.refresh_interval"));
        assertEquals("100", indexService.getIndexSettings().getSettings().get("index.mapping.depth.limit"));

        INDEX_SETTING_DEPTH_ENABLED.set(false);
        INDEX_SETTING_PROVIDER2_ENABLED.set(false);
        INDEX_SETTING_PROVIDER3_ENABLED.set(true);
        var e = expectThrows(IllegalArgumentException.class, () -> createIndex("my-index4", settings));
        assertEquals(
            "additional index setting [index.refresh_interval] added by [TestIndexSettingsProvider] is already present",
            e.getMessage()
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(Plugin1.class, Plugin2.class, Plugin3.class);
    }

    public static class Plugin1 extends Plugin {

        @Override
        public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
            return List.of(new TestIndexSettingsProvider("1s", INDEX_SETTING_PROVIDER1_ENABLED, false));
        }
    }

    public static class Plugin2 extends Plugin {

        @Override
        public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
            return List.of(new TestIndexSettingsProvider("1s", INDEX_SETTING_PROVIDER2_ENABLED, true));
        }
    }

    public static class Plugin3 extends Plugin {

        @Override
        public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
            return List.of(new TestIndexSettingsProvider("100s", INDEX_SETTING_PROVIDER3_ENABLED, false));
        }
    }

    private static final AtomicBoolean INDEX_SETTING_PROVIDER1_ENABLED = new AtomicBoolean(false);
    private static final AtomicBoolean INDEX_SETTING_PROVIDER2_ENABLED = new AtomicBoolean(false);
    private static final AtomicBoolean INDEX_SETTING_PROVIDER3_ENABLED = new AtomicBoolean(false);
    private static final AtomicBoolean INDEX_SETTING_DEPTH_ENABLED = new AtomicBoolean(true);

    static class TestIndexSettingsProvider implements IndexSettingProvider {

        private final String intervalValue;
        private final AtomicBoolean enabled;
        private final boolean overruling;

        TestIndexSettingsProvider(String intervalValue, AtomicBoolean enabled, boolean overruling) {
            this.intervalValue = intervalValue;
            this.enabled = enabled;
            this.overruling = overruling;
        }

        @Override
        public Settings getAdditionalIndexSettings(
            String indexName,
            String dataStreamName,
            IndexMode templateIndexMode,
            Metadata metadata,
            Instant resolvedAt,
            Settings incomingSettings,
            List<CompressedXContent> combinedTemplateMappings
        ) {
            if (enabled.get()) {
                var builder = Settings.builder().put("index.refresh_interval", intervalValue);
                if (INDEX_SETTING_DEPTH_ENABLED.get()) {
                    // Verify that the value can be passed from non-overruling to overruling instance.
                    builder.put("index.mapping.depth.limit", incomingSettings.hasValue("index.refresh_interval") ? 100 : 1);
                }
                return builder.build();
            } else {
                return Settings.EMPTY;
            }
        }

        @Override
        public boolean overrulesSettings() {
            return overruling;
        }
    }
}
