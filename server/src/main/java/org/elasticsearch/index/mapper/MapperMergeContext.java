/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds context used when merging mappings.
 * As the merge process also involves building merged {@link Mapper.Builder}s,
 * this also contains a {@link MapperBuilderContext}.
 */
public final class MapperMergeContext {

    private final MapperBuilderContext mapperBuilderContext;
    private final AtomicLong remainingFieldsUntilLimit;

    public static MapperMergeContext root(boolean isSourceSynthetic, boolean isDataStream, long maxFieldsToAddDuringMerge) {
        return new MapperMergeContext(
            MapperBuilderContext.root(isSourceSynthetic, isDataStream),
            new AtomicLong(maxFieldsToAddDuringMerge)
        );
    }

    public static MapperMergeContext from(MapperBuilderContext mapperBuilderContext, long maxFieldsToAddDuringMerge) {
        return new MapperMergeContext(mapperBuilderContext, new AtomicLong(maxFieldsToAddDuringMerge));
    }

    private MapperMergeContext(MapperBuilderContext mapperBuilderContext, AtomicLong remainingFieldsUntilLimit) {
        this.mapperBuilderContext = mapperBuilderContext;
        this.remainingFieldsUntilLimit = remainingFieldsUntilLimit;
    }

    public MapperMergeContext createChildContext(String name) {
        return createChildContext(mapperBuilderContext.createChildContext(name));
    }

    public MapperMergeContext createChildContext(MapperBuilderContext childContext) {
        return new MapperMergeContext(childContext, remainingFieldsUntilLimit);
    }

    public MapperBuilderContext getMapperBuilderContext() {
        return mapperBuilderContext;
    }

    public void removeRuntimeField(Map<String, RuntimeField> runtimeFields, String name) {
        if (runtimeFields.containsKey(name)) {
            runtimeFields.remove(name);
            if (remainingFieldsUntilLimit.get() != Long.MAX_VALUE) {
                remainingFieldsUntilLimit.incrementAndGet();
            }
        }
    }

    public void addRuntimeFieldIfPossible(Map<String, RuntimeField> runtimeFields, RuntimeField runtimeField) {
        if (runtimeFields.containsKey(runtimeField.name())) {
            runtimeFields.put(runtimeField.name(), runtimeField);
        } else if (canAddField(1)) {
            remainingFieldsUntilLimit.decrementAndGet();
            runtimeFields.put(runtimeField.name(), runtimeField);
        }
    }

    public boolean addFieldIfPossible(Map<String, Mapper> mappers, Mapper mapper) {
        if (canAddField(mapper.mapperSize())) {
            remainingFieldsUntilLimit.getAndAdd(mapper.mapperSize() * -1);
            mappers.put(mapper.simpleName(), mapper);
            return true;
        }
        return false;
    }

    public void addFieldIfPossible(Mapper mapper, Runnable addField) {
        if (canAddField(mapper.mapperSize())) {
            remainingFieldsUntilLimit.getAndAdd(mapper.mapperSize() * -1);
            addField.run();
        }
    }

    public boolean canAddField(int fieldSize) {
        return remainingFieldsUntilLimit.get() >= fieldSize;
    }
}
