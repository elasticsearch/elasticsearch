/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.FieldSubsetReader;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition.FieldGrantExcludeGroup;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.lucene.util.automaton.Operations.subsetOf;

/**
 * Stores patterns to fields which access is granted or denied to and maintains an automaton that can be used to check if permission is
 * allowed for a specific field.
 * Field permissions are configured via a list of strings that are patterns a field has to match. Two lists determine whether or
 * not a field is granted access to:
 * 1. It has to match the patterns in grantedFieldsArray
 * 2. it must not match the patterns in deniedFieldsArray
 */
public final class FieldPermissions implements Accountable {

    public static final FieldPermissions DEFAULT = new FieldPermissions();

    private static final long BASE_FIELD_PERM_DEF_BYTES = RamUsageEstimator.shallowSizeOf(new FieldPermissionsDefinition(null, null));
    private static final long BASE_FIELD_GROUP_BYTES = RamUsageEstimator.shallowSizeOf(new FieldGrantExcludeGroup(null, null));
    private static final long BASE_HASHSET_SIZE = RamUsageEstimator.shallowSizeOfInstance(HashSet.class);
    private static final long BASE_HASHSET_ENTRY_SIZE;
    static {
        HashMap<String, Object> map = new HashMap<>();
        map.put(FieldPermissions.class.getName(), new Object());
        long mapEntryShallowSize = RamUsageEstimator.shallowSizeOf(map.entrySet().iterator().next());
        // assume a load factor of 50%
        // for each entry, we need two object refs, one for the entry itself
        // and one for the free space that is due to the fact hash tables can
        // not be fully loaded
        BASE_HASHSET_ENTRY_SIZE = mapEntryShallowSize + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    }

    private final FieldPermissionsDefinition fieldPermissionsDefinition;
    // an automaton that represents a union of one more sets of permitted and denied fields
    private final CharacterRunAutomaton permittedFieldsAutomaton;
    private final boolean permittedFieldsAutomatonIsTotal;
    private final Automaton originalAutomaton;

    private final long ramBytesUsed;

    /** Constructor that does not enable field-level security: all fields are accepted. */
    public FieldPermissions() {
        this(new FieldPermissionsDefinition(null, null), Automatons.MATCH_ALL);
    }

    /** Constructor that enables field-level security based on include/exclude rules. Exclude rules
     *  have precedence over include rules. */
    public FieldPermissions(FieldPermissionsDefinition fieldPermissionsDefinition) {
        this(fieldPermissionsDefinition, initializePermittedFieldsAutomaton(fieldPermissionsDefinition));
    }

    /** Constructor that enables field-level security based on include/exclude rules. Exclude rules
     *  have precedence over include rules. */
    FieldPermissions(FieldPermissionsDefinition fieldPermissionsDefinition, Automaton permittedFieldsAutomaton) {
        if (permittedFieldsAutomaton.isDeterministic() == false && permittedFieldsAutomaton.getNumStates() > 1) {
            // we only accept deterministic automata so that the CharacterRunAutomaton constructor
            // directly wraps the provided automaton
            throw new IllegalArgumentException("Only accepts deterministic automata");
        }
        this.fieldPermissionsDefinition = fieldPermissionsDefinition;
        this.originalAutomaton = permittedFieldsAutomaton;
        this.permittedFieldsAutomaton = new CharacterRunAutomaton(permittedFieldsAutomaton);
        // we cache the result of isTotal since this might be a costly operation
        this.permittedFieldsAutomatonIsTotal = Operations.isTotal(permittedFieldsAutomaton);

        long ramBytesUsed = BASE_FIELD_PERM_DEF_BYTES;

        if (fieldPermissionsDefinition != null) {
            for (FieldGrantExcludeGroup group : fieldPermissionsDefinition.getFieldGrantExcludeGroups()) {
                ramBytesUsed += BASE_FIELD_GROUP_BYTES + BASE_HASHSET_ENTRY_SIZE;
                if (group.getGrantedFields() != null) {
                    ramBytesUsed += RamUsageEstimator.shallowSizeOf(group.getGrantedFields());
                }
                if (group.getExcludedFields() != null) {
                    ramBytesUsed += RamUsageEstimator.shallowSizeOf(group.getExcludedFields());
                }
            }
        }
        ramBytesUsed += permittedFieldsAutomaton.ramBytesUsed();
        ramBytesUsed += runAutomatonRamBytesUsed(permittedFieldsAutomaton);
        this.ramBytesUsed = ramBytesUsed;
    }

    /**
     * Return an estimation of the ram bytes used by a {@link CharacterRunAutomaton}
     * that wraps the given automaton.
     */
    private static long runAutomatonRamBytesUsed(Automaton a) {
        return a.getNumStates() * 5; // wild guess, better than 0
    }

    public static Automaton initializePermittedFieldsAutomaton(FieldPermissionsDefinition fieldPermissionsDefinition) {
        Set<FieldGrantExcludeGroup> groups = fieldPermissionsDefinition.getFieldGrantExcludeGroups();
        assert groups.size() > 0 : "there must always be a single group for field inclusion/exclusion";
        List<Automaton> automatonList =
                groups.stream()
                        .map(g -> FieldPermissions.initializePermittedFieldsAutomaton(g.getGrantedFields(), g.getExcludedFields()))
                        .collect(Collectors.toList());
        return Automatons.unionAndMinimize(automatonList);
    }

    private static Automaton initializePermittedFieldsAutomaton(final String[] grantedFields, final String[] deniedFields) {
        Automaton grantedFieldsAutomaton;
        if (grantedFields == null || Arrays.stream(grantedFields).anyMatch(Regex::isMatchAllPattern)) {
            grantedFieldsAutomaton = Automatons.MATCH_ALL;
        } else {
            // an automaton that includes metadata fields, including join fields created by the _parent field such
            // as _parent#type
            Automaton metaFieldsAutomaton = Operations.concatenate(Automata.makeChar('_'), Automata.makeAnyString());
            grantedFieldsAutomaton = Operations.union(Automatons.patterns(grantedFields), metaFieldsAutomaton);
        }

        Automaton deniedFieldsAutomaton;
        if (deniedFields == null || deniedFields.length == 0) {
            deniedFieldsAutomaton = Automatons.EMPTY;
        } else {
            deniedFieldsAutomaton = Automatons.patterns(deniedFields);
        }

        grantedFieldsAutomaton = MinimizationOperations.minimize(grantedFieldsAutomaton, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
        deniedFieldsAutomaton = MinimizationOperations.minimize(deniedFieldsAutomaton, Operations.DEFAULT_MAX_DETERMINIZED_STATES);

        if (subsetOf(deniedFieldsAutomaton, grantedFieldsAutomaton) == false) {
            throw new ElasticsearchSecurityException("Exceptions for field permissions must be a subset of the " +
                    "granted fields but " + Strings.arrayToCommaDelimitedString(deniedFields) + " is not a subset of " +
                    Strings.arrayToCommaDelimitedString(grantedFields));
        }

        if (!containsAllField(grantedFields) && !containsAllField(deniedFields)) {
            // It is not explicitly stated whether _all should be allowed/denied
            // In that case we automatically disable _all, unless all fields would match
            if (Operations.isTotal(grantedFieldsAutomaton) && Operations.isEmpty(deniedFieldsAutomaton)) {
                // all fields are accepted, so using _all is fine
            } else {
                deniedFieldsAutomaton = Operations.union(deniedFieldsAutomaton, Automata.makeString(AllFieldMapper.NAME));
            }
        }

        grantedFieldsAutomaton = Automatons.minusAndMinimize(grantedFieldsAutomaton, deniedFieldsAutomaton);
        return grantedFieldsAutomaton;
    }

    private static boolean containsAllField(String[] fields) {
        return fields != null && Arrays.stream(fields).anyMatch(AllFieldMapper.NAME::equals);
    }

    /**
     * Returns a field permissions instance where it is limited by the given field permissions.<br>
     * If the current and the other field permissions have field level security then it takes
     * an intersection of permitted fields.<br>
     * If none of the permissions have field level security enabled, then returns permissions
     * instance where all fields are allowed.
     *
     * @param limitedBy {@link FieldPermissions} used to limit current field permissions
     * @return {@link FieldPermissions}
     */
    public FieldPermissions limitFieldPermissions(FieldPermissions limitedBy) {
        if (hasFieldLevelSecurity() && limitedBy != null && limitedBy.hasFieldLevelSecurity()) {
            Automaton permittedFieldsAutomaton = Automatons.intersectAndMinimize(getIncludeAutomaton(), limitedBy.getIncludeAutomaton());
            return new FieldPermissions(null, permittedFieldsAutomaton);
        } else if (limitedBy != null && limitedBy.hasFieldLevelSecurity()) {
            return new FieldPermissions(limitedBy.getFieldPermissionsDefinition(), limitedBy.getIncludeAutomaton());
        } else if (hasFieldLevelSecurity()) {
            return new FieldPermissions(getFieldPermissionsDefinition(), getIncludeAutomaton());
        }
        return FieldPermissions.DEFAULT;
    }

    /**
     * Returns true if this field permission policy allows access to the field and false if not.
     * fieldName can be a wildcard.
     */
    public boolean grantsAccessTo(String fieldName) {
        return permittedFieldsAutomatonIsTotal || permittedFieldsAutomaton.run(fieldName);
    }

    public FieldPermissionsDefinition getFieldPermissionsDefinition() {
        return fieldPermissionsDefinition;
    }

    /** Return whether field-level security is enabled, ie. whether any field might be filtered out. */
    public boolean hasFieldLevelSecurity() {
        return permittedFieldsAutomatonIsTotal == false;
    }

    /** Return a wrapped reader that only exposes allowed fields. */
    public DirectoryReader filter(DirectoryReader reader) throws IOException {
        if (hasFieldLevelSecurity() == false) {
            return reader;
        }
        return FieldSubsetReader.wrap(reader, permittedFieldsAutomaton);
    }

    Automaton getIncludeAutomaton() {
        return originalAutomaton;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldPermissions that = (FieldPermissions) o;

        if (permittedFieldsAutomatonIsTotal != that.permittedFieldsAutomatonIsTotal) return false;
        return fieldPermissionsDefinition != null ?
                fieldPermissionsDefinition.equals(that.fieldPermissionsDefinition) : that.fieldPermissionsDefinition == null;
    }

    @Override
    public int hashCode() {
        int result = fieldPermissionsDefinition != null ? fieldPermissionsDefinition.hashCode() : 0;
        result = 31 * result + (permittedFieldsAutomatonIsTotal ? 1 : 0);
        return result;
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }
}
