/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * This is a workaround for when compute engine executes concurrently with data partitioning by docid (inter segment parallelization).
 * A {@link SourceProvider} can only be used by a single thread and this wrapping source provider ensures that each thread uses
 * its own {@link SourceProvider}.
 */
final class ReinitializingSourceProvider implements SourceProvider {

    private final Supplier<SourceProvider> sourceProviderFactory;
    private final Map<Long, SourceProvider> map = ConcurrentCollections.newConcurrentMap();

    ReinitializingSourceProvider(Supplier<SourceProvider> sourceProviderFactory) {
        this.sourceProviderFactory = sourceProviderFactory;
    }

    @Override
    public Source getSource(LeafReaderContext ctx, int doc) throws IOException {
        var currentThread = Thread.currentThread();
        var sourceProvider = map.computeIfAbsent(currentThread.threadId(), (key) -> sourceProviderFactory.get());
        return sourceProvider.getSource(ctx, doc);
    }
}
