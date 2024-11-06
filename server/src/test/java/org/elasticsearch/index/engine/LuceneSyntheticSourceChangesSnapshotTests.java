/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;

import static org.elasticsearch.index.mapper.SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING;

public class LuceneSyntheticSourceChangesSnapshotTests extends SearchBasedChangesSnapshotTests {
    @Override
    protected Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.name())
            .put(IndexSettings.INDICES_RECOVERY_SOURCE_SYNTHETIC_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @Override
    protected Translog.Snapshot newRandomSnapshot(
        MappingLookup mappingLookup,
        Engine.Searcher engineSearcher,
        int searchBatchSize,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean singleConsumer,
        boolean accessStats,
        IndexVersion indexVersionCreated
    ) throws IOException {
        return new LuceneSyntheticSourceChangesSnapshot(
            mappingLookup,
            engineSearcher,
            searchBatchSize,
            randomLongBetween(1, LuceneSyntheticSourceChangesSnapshot.DEFAULT_MEMORY_SIZE),
            fromSeqNo,
            toSeqNo,
            requiredFullRange,
            accessStats,
            indexVersionCreated
        );
    }
}
