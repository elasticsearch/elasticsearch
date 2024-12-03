/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Defines which stored fields need to be loaded during a fetch
 * @param requiresSource        should source be loaded
 * @param requiredStoredFields  a set of stored fields to load
 * @param hasScript             Whether scripts or runtime fields are used
 */
public record StoredFieldsSpec(boolean requiresSource, boolean requiresMetadata, Set<String> requiredStoredFields, boolean hasScript) {

    public StoredFieldsSpec(boolean requiresSource, boolean requiresMetadata, Set<String> requiredStoredFields) {
        this(requiresSource, requiresMetadata, requiredStoredFields, false);
    }

    public boolean noRequirements() {
        return requiresSource == false && requiresMetadata == false && requiredStoredFields.isEmpty();
    }

    /**
     * Use when no stored fields are required
     */
    public static final StoredFieldsSpec NO_REQUIREMENTS = new StoredFieldsSpec(false, false, Set.of());

    /**
     * Use when scripts / runtime fields are used and no stored fields are required
     */
    public static final StoredFieldsSpec SCRIPT_NO_REQUIREMENTS = new StoredFieldsSpec(false, false, Set.of(), true);

    /**
     * Use when the source should be loaded but no other stored fields are required
     */
    public static final StoredFieldsSpec NEEDS_SOURCE = new StoredFieldsSpec(true, false, Set.of());

    /**
     * Combine these stored field requirements with those from another StoredFieldsSpec
     */
    public StoredFieldsSpec merge(StoredFieldsSpec other) {
        if (this == other) {
            return this;
        }
        Set<String> mergedFields = new HashSet<>(this.requiredStoredFields);
        mergedFields.addAll(other.requiredStoredFields);
        return new StoredFieldsSpec(
            this.requiresSource || other.requiresSource,
            this.requiresMetadata || other.requiresMetadata,
            mergedFields,
            this.hasScript || other.hasScript
        );
    }

    public static <T> StoredFieldsSpec build(Collection<T> sources, Function<T, StoredFieldsSpec> converter) {
        StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
        for (T source : sources) {
            storedFieldsSpec = storedFieldsSpec.merge(converter.apply(source));
        }
        return storedFieldsSpec;
    }
}
