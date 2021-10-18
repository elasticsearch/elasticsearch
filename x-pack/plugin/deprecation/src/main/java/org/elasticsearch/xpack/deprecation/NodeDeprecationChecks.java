/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.monitoring.MonitoringDeprecatedSettings;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.RESERVED_REALM_NAME_PREFIX;

public class NodeDeprecationChecks {

    static DeprecationIssue checkRemovedSetting(final Settings settings, final Setting<?> removedSetting, final String url) {
        if (removedSetting.exists(settings) == false) {
            return null;
        }
        final String removedSettingKey = removedSetting.getKey();
        final String value = removedSetting.get(settings).toString();
        final String message =
            String.format(Locale.ROOT, "setting [%s] is deprecated and will be removed in the next major version", removedSettingKey);
        final String details =
            String.format(Locale.ROOT, "the setting [%s] is currently set to [%s], remove this setting", removedSettingKey, value);
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    static DeprecationIssue checkSharedDataPathSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
        if (Environment.PATH_SHARED_DATA_SETTING.exists(settings)) {
            final String message = String.format(Locale.ROOT,
                "setting [%s] is deprecated and will be removed in a future version", Environment.PATH_SHARED_DATA_SETTING.getKey());
            final String url = "https://www.elastic.co/guide/en/elasticsearch/reference/7.13/" +
                "breaking-changes-7.13.html#deprecate-shared-data-path-setting";
            final String details = "Found shared data path configured. Discontinue use of this setting.";
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
        }
        return null;
    }

    static DeprecationIssue checkReservedPrefixedRealmNames(final Settings settings, final PluginsAndModules pluginsAndModules) {
        final Map<RealmConfig.RealmIdentifier, Settings> realmSettings = RealmSettings.getRealmSettings(settings);
        if (realmSettings.isEmpty()) {
            return null;
        }
        List<RealmConfig.RealmIdentifier> reservedPrefixedRealmIdentifiers = new ArrayList<>();
        for (RealmConfig.RealmIdentifier realmIdentifier : realmSettings.keySet()) {
            if (realmIdentifier.getName().startsWith(RESERVED_REALM_NAME_PREFIX)) {
                reservedPrefixedRealmIdentifiers.add(realmIdentifier);
            }
        }
        if (reservedPrefixedRealmIdentifiers.isEmpty()) {
            return null;
        } else {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Realm that start with [" + RESERVED_REALM_NAME_PREFIX + "] will not be permitted in a future major release.",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.14/deprecated-7.14.html#reserved-prefixed-realm-names",
                String.format(Locale.ROOT,
                    "Found realm " + (reservedPrefixedRealmIdentifiers.size() == 1 ? "name" : "names")
                        + " with reserved prefix [%s]: [%s]. "
                        + "In a future major release, node will fail to start if any realm names start with reserved prefix.",
                    RESERVED_REALM_NAME_PREFIX,
                    reservedPrefixedRealmIdentifiers.stream()
                        .map(rid -> RealmSettings.PREFIX + rid.getType() + "." + rid.getName())
                        .sorted()
                        .collect(Collectors.joining("; "))), false, null);
        }
    }

    static DeprecationIssue checkSingleDataNodeWatermarkSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
        if (DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.exists(settings)) {
            String key = DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.getKey();
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                String.format(Locale.ROOT, "setting [%s] is deprecated and will not be available in a future version", key),
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.14/" +
                    "breaking-changes-7.14.html#deprecate-single-data-node-watermark",
                String.format(Locale.ROOT, "found [%s] configured. Discontinue use of this setting.", key),
                    false, null);
        }

        return null;
    }

    private static DeprecationIssue deprecatedAffixSetting(Setting.AffixSetting<?> deprecatedAffixSetting, String detailPattern,
                                                           String url, DeprecationIssue.Level warningLevel, Settings settings) {
        List<Setting<?>> deprecatedConcreteSettings = deprecatedAffixSetting.getAllConcreteSettings(settings)
            .sorted(Comparator.comparing(Setting::getKey)).collect(Collectors.toList());

        if (deprecatedConcreteSettings.isEmpty()) {
            return null;
        }

        final String concatSettingNames = deprecatedConcreteSettings.stream().map(Setting::getKey).collect(Collectors.joining(","));
        final String message = String.format(
            Locale.ROOT,
            "settings [%s] are deprecated and will be removed in a future release",
            concatSettingNames
        );
        final String details = String.format(Locale.ROOT, detailPattern, concatSettingNames);

        return new DeprecationIssue(warningLevel, message, url, details, false, null);
    }

    static DeprecationIssue checkExporterUseIngestPipelineSettings(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return deprecatedAffixSetting(MonitoringDeprecatedSettings.USE_INGEST_PIPELINE_SETTING,
            "Remove the following settings from elasticsearch.yml: [%s]",
            "https://ela.st/es-deprecation-7-monitoring-exporter-use-ingest-setting",
            DeprecationIssue.Level.WARNING,
            settings);
    }

    static DeprecationIssue checkExporterPipelineMasterTimeoutSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return deprecatedAffixSetting(MonitoringDeprecatedSettings.PIPELINE_CHECK_TIMEOUT_SETTING,
            "Remove the following settings from elasticsearch.yml: [%s]",
            "https://ela.st/es-deprecation-7-monitoring-exporter-pipeline-timeout-setting",
            DeprecationIssue.Level.WARNING,
            settings);
    }

    static DeprecationIssue checkExporterCreateLegacyTemplateSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return deprecatedAffixSetting(MonitoringDeprecatedSettings.TEMPLATE_CREATE_LEGACY_VERSIONS_SETTING,
            "Remove the following settings from elasticsearch.yml: [%s]",
            "https://ela.st/es-deprecation-7-monitoring-exporter-create-legacy-template-setting",
            DeprecationIssue.Level.WARNING,
            settings);
    }
}
