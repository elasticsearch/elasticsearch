/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A {@link CollectorManager} that takes another CollectorManager as input and wraps all Collectors generated by it
 * in an {@link InternalProfileCollector}. It delegates all the profiling to the generated collectors via {@link #getCollectorTree()}
 * and joins them up when its {@link #reduce} method is called. The profile result can
 */
public final class ProfileCollectorManager<T> implements CollectorManager<InternalProfileCollector, T> {

    private final CollectorManager<Collector, T> collectorManager;
    private final String reason;
    private CollectorResult collectorTree;

    @SuppressWarnings("unchecked")
    public ProfileCollectorManager(CollectorManager<? extends Collector, T> collectorManager, String reason) {
        this.collectorManager = (CollectorManager<Collector, T>) collectorManager;
        this.reason = reason;
    }

    @Override
    public InternalProfileCollector newCollector() throws IOException {
        return new InternalProfileCollector(collectorManager.newCollector(), reason);
    }

    public T reduce(Collection<InternalProfileCollector> profileCollectors) throws IOException {
        List<Collector> unwrapped = profileCollectors.stream()
            .map(InternalProfileCollector::getWrappedCollector)
            .collect(Collectors.toList());
        T returnValue = collectorManager.reduce(unwrapped);

        List<CollectorResult> resultsPerProfiler = profileCollectors.stream()
            .map(ipc -> ipc.getCollectorTree())
            .collect(Collectors.toList());

        long totalTime = resultsPerProfiler.stream().map(CollectorResult::getTime).reduce(0L, Long::sum);
        String collectorName;
        if (resultsPerProfiler.size() == 0) {
            // in case no new collector was ever requested, create a new one just to get the name.
            collectorName = newCollector().getName();
        } else {
            collectorName = resultsPerProfiler.get(0).getName();
        }
        this.collectorTree = new CollectorResult(collectorName, reason, totalTime, Collections.emptyList());
        return returnValue;
    }

    public CollectorResult getCollectorTree() {
        if (this.collectorTree == null) {
            throw new IllegalStateException("A collectorTree hasn't been set yet. Call reduce() before attempting to retrieve it");
        }
        return this.collectorTree;
    }
}
