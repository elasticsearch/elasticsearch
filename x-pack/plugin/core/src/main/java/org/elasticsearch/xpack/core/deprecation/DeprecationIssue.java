/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Information about deprecated items
 */
public class DeprecationIssue implements Writeable, ToXContentObject {

    private static final String ACTIONS_META_FIELD = "actions";
    private static final String OBJECTS_FIELD = "objects";
    private static final String ACTION_TYPE = "action_type";
    private static final String REMOVE_SETTINGS_ACTION_TYPE = "remove_settings";

    public enum Level implements Writeable {
        /**
         * Resolving this issue is advised but not required to upgrade. There may be undesired changes in behavior unless this issue is
         * resolved before upgrading.
         */
        WARNING,
        /**
         * This issue must be resolved to upgrade. Failures will occur unless this is resolved before upgrading.
         */
        CRITICAL;

        public static Level fromString(String value) {
            return Level.valueOf(value.toUpperCase(Locale.ROOT));
        }

        public static Level readFromStream(StreamInput in) throws IOException {
            int ordinal = in.readVInt();
            if (ordinal < 0 || ordinal >= values().length) {
                throw new IOException("Unknown Level ordinal [" + ordinal + "]");
            }
            return values()[ordinal];
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(ordinal());
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final Level level;
    private final String message;
    private final String url;
    private final String details;
    private final boolean resolveDuringRollingUpgrade;
    private final Map<String, Object> meta;

    public DeprecationIssue(
        Level level,
        String message,
        String url,
        @Nullable String details,
        boolean resolveDuringRollingUpgrade,
        @Nullable Map<String, Object> meta
    ) {
        this.level = level;
        this.message = message;
        this.url = url;
        this.details = details;
        this.resolveDuringRollingUpgrade = resolveDuringRollingUpgrade;
        this.meta = meta;
    }

    public DeprecationIssue(StreamInput in) throws IOException {
        level = Level.readFromStream(in);
        message = in.readString();
        url = in.readString();
        details = in.readOptionalString();
        resolveDuringRollingUpgrade = in.getVersion().onOrAfter(Version.V_7_15_0) && in.readBoolean();
        meta = in.getVersion().onOrAfter(Version.V_7_14_0) ? in.readMap() : null;
    }

    public Level getLevel() {
        return level;
    }

    public String getMessage() {
        return message;
    }

    public String getUrl() {
        return url;
    }

    public String getDetails() {
        return details;
    }

    /**
     * @return whether a deprecation issue can only be resolved during a rolling upgrade when a node is offline.
     */
    public boolean isResolveDuringRollingUpgrade() {
        return resolveDuringRollingUpgrade;
    }

    /**
     * @return custom metadata, which allows the ui to display additional details
     *         without parsing the deprecation message itself.
     */
    public Map<String, Object> getMeta() {
        return meta;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        level.writeTo(out);
        out.writeString(message);
        out.writeString(url);
        out.writeOptionalString(details);
        if (out.getVersion().onOrAfter(Version.V_7_15_0)) {
            out.writeBoolean(resolveDuringRollingUpgrade);
        }
        if (out.getVersion().onOrAfter(Version.V_7_14_0)) {
            out.writeMap(meta);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("level", level).field("message", message).field("url", url);
        if (details != null) {
            builder.field("details", details);
        }
        builder.field("resolve_during_rolling_upgrade", resolveDuringRollingUpgrade);
        if (meta != null) {
            builder.field("_meta", meta);
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeprecationIssue that = (DeprecationIssue) o;
        return Objects.equals(level, that.level)
            && Objects.equals(message, that.message)
            && Objects.equals(url, that.url)
            && Objects.equals(details, that.details)
            && Objects.equals(resolveDuringRollingUpgrade, that.resolveDuringRollingUpgrade)
            && Objects.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, message, url, details, resolveDuringRollingUpgrade, meta);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static Map<String, Object> createMetaMapForRemovableSettings(List<String> removableSettings) {
        return createMetaMapWithDesiredRemovableSettings(Collections.emptyMap(), removableSettings);
    }

    /**
     * This method returns a DeprecationIssue that has in its meta object the intersection of all auto-removable settings that appear on
     * all of the DeprecationIssues that are passed in. This method assumes that all DeprecationIssues passed in are equal, except for the
     * auto-removable settings in the meta object.
     * @param similarIssues
     * @return
     */
    public static DeprecationIssue getIntersectionOfRemovableSettings(List<DeprecationIssue> similarIssues) {
        if (similarIssues == null || similarIssues.isEmpty()) {
            return null;
        }
        if (similarIssues.size() == 1) {
            return similarIssues.get(0);
        }
        Tuple<List<String>, Boolean> leastCommonRemovableSettingsDeleteSettingsTuple = getLeastCommonRemovableSettings(similarIssues);
        List<String> leastCommonRemovableSettings = leastCommonRemovableSettingsDeleteSettingsTuple.v1();
        boolean hasSettingsThatNeedToBeDeleted = leastCommonRemovableSettingsDeleteSettingsTuple.v2();
        DeprecationIssue representativeIssue = similarIssues.get(0);
        Map<String, Object> representativeMeta = representativeIssue.getMeta();
        final Map<String, Object> newMeta = buildNewMetaMap(
            representativeMeta,
            leastCommonRemovableSettings,
            hasSettingsThatNeedToBeDeleted
        );
        return new DeprecationIssue(
            representativeIssue.level,
            representativeIssue.message,
            representativeIssue.url,
            representativeIssue.details,
            representativeIssue.resolveDuringRollingUpgrade,
            newMeta
        );
    }

    /*
     * This method takes in a representative meta map from a DeprecationIssue, and strips out any settings that are not in the
     * leastCommonRemovableSettings list -- that is, the settings that appeared in all of the DeprecationIssues's meta maps. The
     * hasSettingsThatNeedToBeDeleted is an optimization -- if it is false there is no need to do as much work because we can just return a
     * copy of the representative map.
     */
    private static Map<String, Object> buildNewMetaMap(
        Map<String, Object> representativeMeta,
        List<String> leastCommonRemovableSettings,
        boolean hasSettingsThatNeedToBeDeleted
    ) {
        final Map<String, Object> newMeta;
        if (representativeMeta != null) {
            if (hasSettingsThatNeedToBeDeleted) {
                newMeta = createMetaMapWithDesiredRemovableSettings(representativeMeta, leastCommonRemovableSettings);
            } else {
                newMeta = representativeMeta;
            }
        } else {
            newMeta = null;
        }
        return newMeta;
    }

    /*
     * This method returns a list of all of the Settings that appear in the meta map of all of the similarIssues. It also returns a boolean
     * indicating whether it found any settings that were not in all similarIssues (indicating that the calling code will need to remove
     * some from the meta map).
     */
    private static Tuple<List<String>, Boolean> getLeastCommonRemovableSettings(List<DeprecationIssue> similarIssues) {
        boolean hasSettingsThatNeedToBeDeleted = false;
        List<String> leastCommonRemovableSettings = null;
        for (DeprecationIssue issue : similarIssues) {
            List<String> removableSettings = issue.getRemovableSettings();
            if (removableSettings == null) {
                leastCommonRemovableSettings = null;
                break;
            }
            if (leastCommonRemovableSettings == null) {
                leastCommonRemovableSettings = new ArrayList<>(removableSettings);
            } else {
                List<String> newleastCommonRemovableSettings = leastCommonRemovableSettings.stream()
                    .distinct()
                    .filter(removableSettings::contains)
                    .collect(Collectors.toList());
                if (newleastCommonRemovableSettings.size() != leastCommonRemovableSettings.size()) {
                    hasSettingsThatNeedToBeDeleted = true;
                }
                leastCommonRemovableSettings = newleastCommonRemovableSettings;
            }
        }
        return Tuple.tuple(leastCommonRemovableSettings, hasSettingsThatNeedToBeDeleted);
    }

    @SuppressWarnings("unchecked")
    private List<String> getRemovableSettings() {
        Map<String, Object> meta = getMeta();
        if (meta == null) {
            return null;
        }
        Object actionsObject = meta.get(ACTIONS_META_FIELD);
        if (actionsObject == null) {
            return null;
        }
        List<Map<String, Object>> actionsMapList = (List<Map<String, Object>>) actionsObject;
        if (actionsMapList.isEmpty()) {
            return null;
        }
        for (Map<String, Object> actionsMap : actionsMapList) {
            if (REMOVE_SETTINGS_ACTION_TYPE.equals(actionsMap.get(ACTION_TYPE))) {
                return (List<String>) actionsMap.get(OBJECTS_FIELD);
            }
        }
        return null;
    }

    /*
     * Returns a new meta map based on the one given, but with only the desiredRemovableSettings as removable settings
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> createMetaMapWithDesiredRemovableSettings(
        Map<String, Object> seedMeta,
        List<String> desiredRemovableSettings
    ) {
        Map<String, Object> newMeta = new HashMap<>(seedMeta);
        boolean foundActionToUpdate = false;
        List<Map<String, Object>> actions = (List<Map<String, Object>>) newMeta.get(ACTIONS_META_FIELD);
        final List<Map<String, Object>> clonedActions;
        if (actions == null) {
            clonedActions = new ArrayList<>();
        } else {
            clonedActions = new ArrayList<>(actions); // So that we don't modify the input data
            for (int i = 0; i < clonedActions.size(); i++) {
                Map<String, Object> actionsMap = clonedActions.get(i);
                if (REMOVE_SETTINGS_ACTION_TYPE.equals(actionsMap.get(ACTION_TYPE))) {
                    foundActionToUpdate = true;
                    if (desiredRemovableSettings != null && desiredRemovableSettings.isEmpty() == false) {
                        Map<String, Object> clonedActionsMap = new HashMap<>(actionsMap);
                        clonedActionsMap.put(OBJECTS_FIELD, desiredRemovableSettings);
                        clonedActions.set(i, clonedActionsMap);
                    } else {
                        // If the desired removable settings is null / empty, we don't even want to have the action
                        clonedActions.remove(i);
                        i--;
                    }
                }
            }
        }
        if (foundActionToUpdate == false) { // Either there were no remove_settings, or no just no actions at all
            if (desiredRemovableSettings != null && desiredRemovableSettings.isEmpty() == false) {
                Map<String, Object> actionsMap = new HashMap<>();
                actionsMap.put(ACTION_TYPE, REMOVE_SETTINGS_ACTION_TYPE);
                actionsMap.put(OBJECTS_FIELD, desiredRemovableSettings);
                clonedActions.add(actionsMap);
            }
        }
        newMeta.put(ACTIONS_META_FIELD, clonedActions);
        return newMeta;
    }

}
