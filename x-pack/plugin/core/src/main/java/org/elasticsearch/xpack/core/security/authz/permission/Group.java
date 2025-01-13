/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.READ;
import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.READ_FAILURES_PRIVILEGE_NAME;

/**
 * Represents an {@link IndicesPermission} group, as in one set of index name patterns and the privileges granted for the indices
 * covered by those patterns.
 */
public class Group {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(Group.class);

    public static final Group[] EMPTY_ARRAY = new Group[0];

    final IndexPrivilege privilege; // ATHE make this private again
    private final Predicate<String> actionMatcher;
    private final String[] indices;
    final StringMatcher indexNameMatcher; // ATHE make this private again
    private final Supplier<Automaton> indexNameAutomaton;
    // TODO: Use FieldPermissionsDefinition instead of FieldPermissions. The former is a better counterpart to query
    private final FieldPermissions fieldPermissions;
    private final Set<BytesReference> query;
    // by default certain restricted indices are exempted when granting privileges, as they should generally be hidden for ordinary
    // users. Setting this flag true eliminates the special status for the purpose of this permission - restricted indices still have
    // to be covered by the "indices"
    private final boolean allowRestrictedIndices;
    // These two flags are just a cache so we don't have to constantly re-check strings
    private final boolean hasMappingUpdateBwcPermissions;
    private final boolean hasReadFailuresPrivilege;

    public Group(
        IndexPrivilege privilege,
        FieldPermissions fieldPermissions,
        @Nullable Set<BytesReference> query,
        boolean allowRestrictedIndices,
        RestrictedIndices restrictedIndices,
        String... indices
    ) {
        assert indices.length != 0;
        this.privilege = privilege;
        this.actionMatcher = privilege.predicate();
        this.indices = indices;
        this.allowRestrictedIndices = allowRestrictedIndices;
        ConcurrentHashMap<String[], Automaton> indexNameAutomatonMemo = new ConcurrentHashMap<>(1);
        if (allowRestrictedIndices) {
            this.indexNameMatcher = StringMatcher.of(indices);
            this.indexNameAutomaton = () -> indexNameAutomatonMemo.computeIfAbsent(indices, k -> Automatons.patterns(indices));
        } else {
            this.indexNameMatcher = StringMatcher.of(indices).and(name -> restrictedIndices.isRestricted(name) == false);
            this.indexNameAutomaton = () -> indexNameAutomatonMemo.computeIfAbsent(
                indices,
                k -> Automatons.minusAndMinimize(Automatons.patterns(indices), restrictedIndices.getAutomaton())
            );
        }
        this.fieldPermissions = Objects.requireNonNull(fieldPermissions);
        this.query = query;
        this.hasMappingUpdateBwcPermissions = IndicesPermission.containsPrivilegeThatGrantsMappingUpdatesForBwc(privilege.name());
        this.hasReadFailuresPrivilege = this.privilege.name().stream().anyMatch(READ_FAILURES_PRIVILEGE_NAME::equals);
    }

    public IndexPrivilege privilege() {
        return privilege;
    }

    public String[] indices() {
        return indices;
    }

    @Nullable
    public Set<BytesReference> getQuery() {
        return query;
    }

    public FieldPermissions getFieldPermissions() {
        return fieldPermissions;
    }

    IsResourceAuthorizedPredicate allowedIndicesPredicate(String action) {
        return new IsResourceAuthorizedPredicate(allowedIndicesChecker(action));
    }

    /**
     * Returns a checker which checks if users are permitted to access index abstractions for the given action.
     *
     * @param action
     * @return A function which checks an index name and returns the components of this abstraction the user is authorized to access
     * with the given action.
     */
    private BiFunction<String, IndexAbstraction, IndicesPermission.AuthorizedComponents> allowedIndicesChecker(String action) {
        boolean isReadAction = READ.predicate().test(action);
        boolean isMappingUpdateAction = IndicesPermission.isMappingUpdateAction(action);
        boolean actionMatches = actionMatcher.test(action);
        boolean actionAuthorized = actionMatches || (isReadAction && hasReadFailuresPrivilege);
        if (actionAuthorized == false) {
            AtomicBoolean logged = new AtomicBoolean(false);
            return (name, resource) -> {
                if (isMappingUpdateAction
                    && hasMappingUpdateBwcPermissions
                    && resource != null
                    && resource.getParentDataStream() == null
                    && resource.getType() != IndexAbstraction.Type.DATA_STREAM) {
                    boolean alreadyLogged = logged.getAndSet(true);
                    if (alreadyLogged == false) {
                        for (String privilegeName : this.privilege.name()) {
                            if (IndicesPermission.PRIVILEGE_NAME_SET_BWC_ALLOW_MAPPING_UPDATE.contains(privilegeName)) {
                                deprecationLogger.warn(
                                    DeprecationCategory.SECURITY,
                                    "[" + resource.getName() + "] mapping update for ingest privilege [" + privilegeName + "]",
                                    "the index privilege ["
                                        + privilegeName
                                        + "] allowed the update "
                                        + "mapping action ["
                                        + action
                                        + "] on index ["
                                        + resource.getName()
                                        + "], this privilege "
                                        + "will not permit mapping updates in the next major release - users who require access "
                                        + "to update mappings must be granted explicit privileges"
                                );
                            }
                        }
                    }
                    return IndicesPermission.AuthorizedComponents.ALL;
                }
                return IndicesPermission.AuthorizedComponents.NONE;
            };
        }

        return (String abstractionName, IndexAbstraction resource) -> {
            assert resource == null || abstractionName.equals(resource.getName());
            return switch (resource.getType()) {
                case ALIAS -> checkMultiIndexAbstraction(isReadAction, actionMatches, resource);
                case DATA_STREAM -> checkMultiIndexAbstraction(isReadAction, actionMatches, resource);
                case CONCRETE_INDEX -> {
                    IndexAbstraction.ConcreteIndex index = (IndexAbstraction.ConcreteIndex) resource;
                    final DataStream ds = index.getParentDataStream();

                    if (ds != null) {
                        boolean isFailureStoreIndex = ds.getFailureIndices().containsIndex(resource.getName());

                        if (isReadAction) {
                            // If we're trying to read a failure store index, we need to have read_failures for the data stream
                            if (isFailureStoreIndex) {
                                if ((hasReadFailuresPrivilege && indexNameMatcher.test(ds.getName()))) {
                                    yield IndicesPermission.AuthorizedComponents.FAILURES; // And authorize it as a failures index (i.e. no
                                                                                           // DLS/FLS)
                                }
                            } else { // not a failure store index
                                if (indexNameMatcher.test(ds.getName()) || indexNameMatcher.test(resource.getName())) {
                                    yield IndicesPermission.AuthorizedComponents.DATA;
                                }
                            }
                        } else { // Not a read action, authenticate as normal
                            if (checkParentNameAndResourceName(ds.getName(), resource.getName())) {
                                yield IndicesPermission.AuthorizedComponents.DATA;
                            }
                        }
                    } else if (indexNameMatcher.test(resource.getName())) {
                        yield IndicesPermission.AuthorizedComponents.DATA;
                    }
                    yield IndicesPermission.AuthorizedComponents.NONE;
                }
                case null -> indexNameMatcher.test(abstractionName)
                    ? IndicesPermission.AuthorizedComponents.DATA
                    : IndicesPermission.AuthorizedComponents.NONE;
            };
        };
    }

    private boolean checkParentNameAndResourceName(String dsName, String indexName) {
        return indexNameMatcher.test(dsName) || indexNameMatcher.test(indexName);
    }

    private IndicesPermission.AuthorizedComponents checkMultiIndexAbstraction(
        boolean isReadAction,
        boolean actionMatches,
        IndexAbstraction resource
    ) {
        if (indexNameMatcher.test(resource.getName())) {
            if (actionMatches && (isReadAction == false || hasReadFailuresPrivilege)) {
                // User has both normal read privileges and read_failures OR normal privileges and action is not read
                return IndicesPermission.AuthorizedComponents.ALL;
            } else if (actionMatches && hasReadFailuresPrivilege == false) {
                return IndicesPermission.AuthorizedComponents.DATA;
            } else if (hasReadFailuresPrivilege) { // action not authorized by typical match
                return IndicesPermission.AuthorizedComponents.FAILURES;
            }
        }
        return IndicesPermission.AuthorizedComponents.NONE;
    }

    boolean checkAction(String action) {
        return actionMatcher.test(action);
    }

    boolean checkIndex(String index) {
        assert index != null;
        return indexNameMatcher.test(index);
    }

    boolean hasQuery() {
        return query != null;
    }

    public boolean allowRestrictedIndices() {
        return allowRestrictedIndices;
    }

    public Automaton getIndexMatcherAutomaton() {
        return indexNameAutomaton.get();
    }

    boolean isTotal() {
        return allowRestrictedIndices
            && indexNameMatcher.isTotal()
            && privilege == IndexPrivilege.ALL
            && query == null
            && false == fieldPermissions.hasFieldLevelSecurity();
    }

    @Override
    public String toString() {
        return "Group{"
            + "privilege="
            + privilege
            + ", indices="
            + Strings.arrayToCommaDelimitedString(indices)
            + ", fieldPermissions="
            + fieldPermissions
            + ", query="
            + query
            + ", allowRestrictedIndices="
            + allowRestrictedIndices
            + '}';
    }
}
