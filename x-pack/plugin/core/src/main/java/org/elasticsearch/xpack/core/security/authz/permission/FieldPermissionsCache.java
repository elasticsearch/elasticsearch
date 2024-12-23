/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition.FieldGrantExcludeGroup;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.SecurityField.setting;

/**
 * A service for managing the caching of {@link FieldPermissions} as these may often need to be combined or created and internally they
 * use an {@link org.apache.lucene.util.automaton.Automaton}, which can be costly to create once you account for minimization
 */
public final class FieldPermissionsCache {

    private static final Logger logger = LogManager.getLogger(FieldPermissionsCache.class);

    public static final Setting<Long> CACHE_SIZE_SETTING = Setting.longSetting(
        setting("authz.store.roles.field_permissions.cache.max_size_in_bytes"),
        100 * 1024 * 1024,
        -1L,
        Property.NodeScope
    );
    private final Cache<FieldPermissionsDefinition, FieldPermissions> cache;

    public FieldPermissionsCache(Settings settings) {
        this.cache = CacheBuilder.<FieldPermissionsDefinition, FieldPermissions>builder()
            .setMaximumWeight(CACHE_SIZE_SETTING.get(settings))
            .weigher((key, fieldPermissions) -> fieldPermissions.ramBytesUsed())
            .build();
    }

    public Cache.CacheStats getCacheStats() {
        return cache.stats();
    }

    /**
     * Gets a {@link FieldPermissions} instance that corresponds to the granted and denied parameters. The instance may come from the cache
     * or if it gets created, the instance will be cached
     */
    FieldPermissions getFieldPermissions(String[] granted, String[] denied) {
        return getFieldPermissions(new FieldPermissionsDefinition(granted, denied));
    }

    /**
     * Gets a {@link FieldPermissions} instance that corresponds to the granted and denied parameters. The instance may come from the cache
     * or if it gets created, the instance will be cached
     */
    public FieldPermissions getFieldPermissions(FieldPermissionsDefinition fieldPermissionsDefinition) {
        try {
            return cache.computeIfAbsent(fieldPermissionsDefinition, FieldPermissions::new);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ElasticsearchException es) {
                throw es;
            } else {
                throw new ElasticsearchSecurityException("unable to compute field permissions", e);
            }
        }
    }

    public void validateFieldPermissionsWithCache(String roleName, FieldPermissionsDefinition fieldPermissionsDefinition) {
        try {
            // we're "abusing" the cache here a bit: to avoid excessive WARN logging, only log if the entry is not cached
            cache.computeIfAbsent(fieldPermissionsDefinition, key -> {
                var fieldPermissions = new FieldPermissions(key);
                if (fieldPermissions.hasLegacyExceptionFields()) {
                    logger.warn(
                        "Role [{}] has exceptions for field permissions that cover fields prefixed with [_] "
                            + "but are not a subset of the granted fields. "
                            + "This is supported for backwards compatibility only. "
                            + "To avoid counter-intuitive field-level security behavior, ensure that the [except] field is a subset of the "
                            + "[grant] field by either adding the missing _-prefixed fields to the [grant] field, "
                            + "or by removing them from the [except] field. "
                            + "You cannot exclude any of [{}] since these are minimally required metadata fields.",
                        roleName,
                        Strings.collectionToCommaDelimitedString(new TreeSet<>(FieldPermissions.METADATA_FIELDS_ALLOWLIST))
                    );
                }
                return fieldPermissions;
            });
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ElasticsearchException es) {
                throw es;
            } else {
                throw new ElasticsearchSecurityException("unable to compute field permissions", e);
            }
        }
    }

    /**
     * Returns a field permissions object that corresponds to the union of the given field permissions.
     * Union means a field is granted if it is granted by any of the FieldPermissions from the given
     * collection.
     * The returned instance is cached if one was not found in the cache.
     */
    FieldPermissions union(Collection<FieldPermissions> fieldPermissionsCollection) {
        Optional<FieldPermissions> allowAllFieldPermissions = fieldPermissionsCollection.stream()
            .filter(((Predicate<FieldPermissions>) (FieldPermissions::hasFieldLevelSecurity)).negate())
            .findFirst();
        return allowAllFieldPermissions.orElseGet(() -> {
            final Set<FieldGrantExcludeGroup> fieldGrantExcludeGroups = new HashSet<>();
            boolean hasLegacyExceptFields = false;
            for (FieldPermissions fieldPermissions : fieldPermissionsCollection) {
                final List<FieldPermissionsDefinition> fieldPermissionsDefinitions = fieldPermissions.getFieldPermissionsDefinitions();
                if (fieldPermissionsDefinitions.size() != 1) {
                    throw new IllegalArgumentException(
                        "Expected a single field permission definition, but found [" + fieldPermissionsDefinitions + "]"
                    );
                }
                fieldGrantExcludeGroups.addAll(fieldPermissionsDefinitions.getFirst().getFieldGrantExcludeGroups());
                hasLegacyExceptFields = hasLegacyExceptFields || fieldPermissions.hasLegacyExceptionFields();
            }
            final FieldPermissionsDefinition combined = new FieldPermissionsDefinition(fieldGrantExcludeGroups);
            final boolean hasLegacyExceptFieldsFinal = hasLegacyExceptFields;
            try {
                return cache.computeIfAbsent(combined, (key) -> {
                    List<Automaton> automatonList = fieldPermissionsCollection.stream()
                        .map(FieldPermissions::getIncludeAutomaton)
                        .collect(Collectors.toList());
                    return new FieldPermissions(
                        key,
                        new FieldPermissions.AutomatonWithLegacyExceptionFieldsFlag(
                            Automatons.unionAndDeterminize(automatonList),
                            hasLegacyExceptFieldsFinal
                        )
                    );
                });
            } catch (ExecutionException e) {
                throw new ElasticsearchException("unable to compute field permissions", e);
            }
        });
    }
}
