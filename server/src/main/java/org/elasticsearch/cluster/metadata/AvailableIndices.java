/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class AvailableIndices {

    private final Metadata metadata;
    private final Predicate<IndexAbstraction> predicate;
    private final Consumer<Collection<String>> availableNamesConsumer;
    private Set<String> availableNames;

    public AvailableIndices(Metadata metadata, Predicate<IndexAbstraction> predicate, Consumer<Collection<String>> availableNamesConsumer) {
        this.metadata = Objects.requireNonNull(metadata);
        this.predicate = Objects.requireNonNull(predicate);
        this.availableNamesConsumer = availableNamesConsumer;
    }

    public AvailableIndices(Set<String> availableNames) {
        this.availableNames = availableNames;
        this.metadata = null;
        this.predicate = null;
        this.availableNamesConsumer = null;
    }

    public Set<String> getAvailableNames() {
        if (availableNames == null) {
            availableNames = new HashSet<>();
            assert predicate != null && metadata != null && availableNamesConsumer != null;
            for (IndexAbstraction indexAbstraction : metadata.getIndicesLookup().values()) {
                // Short circuit if it is already authorized. This can happen for data stream backing indices
                if (availableNames.contains(indexAbstraction.getName())) {
                    continue;
                }
                if (predicate.test(indexAbstraction)) {
                    availableNames.add(indexAbstraction.getName());
                    // Data stream guarantees access to its underlying indices
                    if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
                        indexAbstraction.getIndices().forEach(index -> availableNames.add(index.getName()));
                    }
                }
            }
            availableNamesConsumer.accept(availableNames);
        }
        return availableNames;
    }

    public boolean isAvailableName(String name) {
        if (availableNames == null) {
            assert predicate != null && metadata != null;
            return predicate.test(metadata.getIndicesLookup().get(name));
        } else {
            return availableNames.contains(name);
        }
    }
}
