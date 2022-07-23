/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A container to keep settings for disk thresholds up to date with cluster setting changes.
 */
public class DiskThresholdSettings {
    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.disk.threshold_enabled",
        true,
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<RelativeByteSizeValue> CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.low",
        "85%",
        (s) -> RelativeByteSizeValue.parseRelativeByteSizeValue(s, "cluster.routing.allocation.disk.watermark.low"),
        new WatermarkValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.low.max_headroom",
        (settings) -> {
            if (CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.exists(settings)) {
                return "-1";
            } else {
                return "150GB";
            }
        },
        (s) -> ByteSizeValue.parseBytesSizeValue(s, "cluster.routing.allocation.disk.watermark.low.max_headroom"),
        new MaxHeadroomValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<RelativeByteSizeValue> CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.high",
        "90%",
        (s) -> RelativeByteSizeValue.parseRelativeByteSizeValue(s, "cluster.routing.allocation.disk.watermark.high"),
        new WatermarkValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.high.max_headroom",
        (settings) -> {
            if (CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.exists(settings)) {
                return "-1";
            } else {
                return "100GB";
            }
        },
        (s) -> ByteSizeValue.parseBytesSizeValue(s, "cluster.routing.allocation.disk.watermark.high.max_headroom"),
        new MaxHeadroomValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<RelativeByteSizeValue> CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.flood_stage",
        "95%",
        (s) -> RelativeByteSizeValue.parseRelativeByteSizeValue(s, "cluster.routing.allocation.disk.watermark.flood_stage"),
        new WatermarkValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.flood_stage.max_headroom",
        (settings) -> {
            if (CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.exists(settings)) {
                return "-1";
            } else {
                return "20GB";
            }
        },
        (s) -> ByteSizeValue.parseBytesSizeValue(s, "cluster.routing.allocation.disk.watermark.flood_stage.max_headroom"),
        new MaxHeadroomValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<RelativeByteSizeValue> CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.flood_stage.frozen",
        "95%",
        (s) -> RelativeByteSizeValue.parseRelativeByteSizeValue(s, "cluster.routing.allocation.disk.watermark.flood_stage.frozen"),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.flood_stage.frozen.max_headroom",
        (settings) -> {
            if (CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.exists(settings)) {
                return "-1";
            } else {
                return "20GB";
            }
        },
        (s) -> ByteSizeValue.parseBytesSizeValue(s, "cluster.routing.allocation.disk.watermark.flood_stage.frozen.max_headroom"),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "cluster.routing.allocation.disk.reroute_interval",
        TimeValue.timeValueSeconds(60),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile RelativeByteSizeValue lowStageWatermark;
    private volatile ByteSizeValue lowStageMaxHeadroom;
    private volatile RelativeByteSizeValue highStageWatermark;
    private volatile ByteSizeValue highStageMaxHeadroom;
    private volatile RelativeByteSizeValue floodStageWatermark;
    private volatile ByteSizeValue floodStageMaxHeadroom;
    private volatile RelativeByteSizeValue frozenFloodStageWatermark;
    private volatile ByteSizeValue frozenFloodStageMaxHeadroom;
    private volatile boolean enabled;
    private volatile TimeValue rerouteInterval;

    static {
        assert Version.CURRENT.major == Version.V_7_0_0.major + 1; // this check is unnecessary in v9
        final String AUTO_RELEASE_INDEX_ENABLED_KEY = "es.disk.auto_release_flood_stage_block";
        final String property = System.getProperty(AUTO_RELEASE_INDEX_ENABLED_KEY);
        if (property != null) {
            throw new IllegalArgumentException("system property [" + AUTO_RELEASE_INDEX_ENABLED_KEY + "] may not be set");
        }
    }

    public DiskThresholdSettings(Settings settings, ClusterSettings clusterSettings) {
        setLowWatermark(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.get(settings));
        setLowStageMaxHeadroom(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.get(settings));
        setHighWatermark(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.get(settings));
        setHighStageMaxHeadroom(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.get(settings));
        setFloodStageWatermark(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.get(settings));
        setFloodStageMaxHeadroom(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.get(settings));
        setFrozenFloodStageWatermark(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.get(settings));
        setFrozenFloodStageMaxHeadroom(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.get(settings));
        this.rerouteInterval = CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(settings);
        this.enabled = CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING, this::setLowWatermark);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING, this::setLowStageMaxHeadroom);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING, this::setHighWatermark);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING, this::setHighStageMaxHeadroom);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING,
            this::setFloodStageWatermark
        );
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING,
            this::setFloodStageMaxHeadroom
        );
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING,
            this::setFrozenFloodStageWatermark
        );
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING,
            this::setFrozenFloodStageMaxHeadroom
        );
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING, this::setRerouteInterval);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING, this::setEnabled);
    }

    /**
     * Validates that low, high and flood stage watermarks are all either percentages or byte values,
     * and that their values adhere to the comparison: low &lt; high &lt; flood. Else, throws an exception.
     */
    static class WatermarkValidator implements Setting.Validator<RelativeByteSizeValue> {

        @Override
        public void validate(RelativeByteSizeValue value) {

        }

        @Override
        public void validate(final RelativeByteSizeValue value, final Map<Setting<?>, Object> settings) {
            final RelativeByteSizeValue low = (RelativeByteSizeValue) settings.get(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING);
            final RelativeByteSizeValue high = (RelativeByteSizeValue) settings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING);
            final RelativeByteSizeValue flood = (RelativeByteSizeValue) settings.get(
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING
            );

            if (low.isAbsolute() == false && high.isAbsolute() == false && flood.isAbsolute() == false) { // Validate as percentages
                final double lowWatermarkThreshold = low.getRatio().getAsPercent();
                final double highWatermarkThreshold = high.getRatio().getAsPercent();
                final double floodThreshold = flood.getRatio().getAsPercent();
                if (lowWatermarkThreshold > highWatermarkThreshold) {
                    throw new IllegalArgumentException(
                        "low disk watermark [" + low.getStringRep() + "] more than high disk watermark [" + high.getStringRep() + "]"
                    );
                }
                if (highWatermarkThreshold > floodThreshold) {
                    throw new IllegalArgumentException(
                        "high disk watermark ["
                            + high.getStringRep()
                            + "] more than flood stage disk watermark ["
                            + flood.getStringRep()
                            + "]"
                    );
                }
            } else if (low.isAbsolute() && high.isAbsolute() && flood.isAbsolute()) { // Validate as absolute values
                final ByteSizeValue lowWatermarkBytes = low.getAbsolute();
                final ByteSizeValue highWatermarkBytes = high.getAbsolute();
                final ByteSizeValue floodStageBytes = flood.getAbsolute();

                if (lowWatermarkBytes.getBytes() < highWatermarkBytes.getBytes()) {
                    throw new IllegalArgumentException(
                        "low disk watermark [" + low.getStringRep() + "] less than high disk watermark [" + high.getStringRep() + "]"
                    );
                }
                if (highWatermarkBytes.getBytes() < floodStageBytes.getBytes()) {
                    throw new IllegalArgumentException(
                        "high disk watermark ["
                            + high.getStringRep()
                            + "] less than flood stage disk watermark ["
                            + flood.getStringRep()
                            + "]"
                    );
                }
            } else {
                final String message = Strings.format(
                    "unable to consistently parse [%s=%s], [%s=%s], and [%s=%s] as percentage or bytes",
                    CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                    low.getStringRep(),
                    CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                    high.getStringRep(),
                    CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                    flood.getStringRep()
                );
                throw new IllegalArgumentException(message);
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(
                CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING,
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING,
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING
            );
            return settings.iterator();
        }

    }

    /**
     * Validates that low, high and flood stage max headrooms adhere to the comparison: flood &lt; high &lt; low. Else, throws an exception.
     */
    static class MaxHeadroomValidator implements Setting.Validator<ByteSizeValue> {

        @Override
        public void validate(ByteSizeValue value) {

        }

        @Override
        public void validate(final ByteSizeValue value, final Map<Setting<?>, Object> settings) {
            final ByteSizeValue lowHeadroom = (ByteSizeValue) settings.get(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING);
            final ByteSizeValue highHeadroom = (ByteSizeValue) settings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING);
            final ByteSizeValue floodHeadroom = (ByteSizeValue) settings.get(
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING
            );

            // For the comparisons, we need to mind that headroom values can default to -1.
            if (floodHeadroom.compareTo(highHeadroom) > 0 && highHeadroom.getBytes() > 0) {
                throw new IllegalArgumentException(
                    "flood disk max headroom ["
                        + floodHeadroom.getStringRep()
                        + "] more than high disk max headroom ["
                        + highHeadroom.getStringRep()
                        + "]"
                );
            }
            if (highHeadroom.compareTo(lowHeadroom) > 0 && lowHeadroom.getBytes() > 0) {
                throw new IllegalArgumentException(
                    "high disk max headroom ["
                        + highHeadroom.getStringRep()
                        + "] more than low disk max headroom ["
                        + lowHeadroom.getStringRep()
                        + "]"
                );
            }
            // This check is also needed in case high headroom is -1 and relevant check above was skipped.
            if (floodHeadroom.compareTo(lowHeadroom) > 0 && lowHeadroom.getBytes() > 0) {
                throw new IllegalArgumentException(
                    "flood disk max headroom ["
                        + floodHeadroom.getStringRep()
                        + "] more than low disk max headroom ["
                        + lowHeadroom.getStringRep()
                        + "]"
                );
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(
                CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING,
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING,
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING
            );
            return settings.iterator();
        }

    }

    private void setRerouteInterval(TimeValue rerouteInterval) {
        this.rerouteInterval = rerouteInterval;
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void setLowWatermark(RelativeByteSizeValue lowWatermark) {
        this.lowStageWatermark = lowWatermark;
    }

    private void setLowStageMaxHeadroom(ByteSizeValue maxHeadroom) {
        this.lowStageMaxHeadroom = maxHeadroom;
    }

    private void setHighWatermark(RelativeByteSizeValue highWatermark) {
        this.highStageWatermark = highWatermark;
    }

    private void setHighStageMaxHeadroom(ByteSizeValue maxHeadroom) {
        this.highStageMaxHeadroom = maxHeadroom;
    }

    private void setFloodStageWatermark(RelativeByteSizeValue floodStage) {
        this.floodStageWatermark = floodStage;
    }

    private void setFloodStageMaxHeadroom(ByteSizeValue maxHeadroom) {
        this.floodStageMaxHeadroom = maxHeadroom;
    }

    private void setFrozenFloodStageWatermark(RelativeByteSizeValue floodStage) {
        this.frozenFloodStageWatermark = floodStage;
    }

    private void setFrozenFloodStageMaxHeadroom(ByteSizeValue maxHeadroom) {
        this.frozenFloodStageMaxHeadroom = maxHeadroom;
    }

    private ByteSizeValue getFreeBytesThreshold(
        ByteSizeValue total,
        RelativeByteSizeValue watermark,
        ByteSizeValue maxHeadroom,
        boolean floorInsteadOfCeil
    ) {
        // If bytes are given, they can be readily returned as free bytes. If percentages are given, we need to calculate the free bytes.
        if (watermark.isAbsolute()) {
            return watermark.getAbsolute();
        }
        return ByteSizeValue.ofBytes(total.getBytes() - watermark.calculateValue(total, maxHeadroom, floorInsteadOfCeil).getBytes());
    }

    public ByteSizeValue getFreeBytesThresholdLowStage(ByteSizeValue total) {
        return getFreeBytesThreshold(total, lowStageWatermark, lowStageMaxHeadroom, false);
    }

    public ByteSizeValue getFreeBytesThresholdHighStage(ByteSizeValue total) {
        return getFreeBytesThreshold(total, highStageWatermark, highStageMaxHeadroom, false);
    }

    public ByteSizeValue getFreeBytesThresholdFloodStage(ByteSizeValue total) {
        return getFreeBytesThreshold(total, floodStageWatermark, floodStageMaxHeadroom, false);
    }

    public ByteSizeValue getFreeBytesThresholdFrozenFloodStage(ByteSizeValue total) {
        return getFreeBytesThreshold(total, frozenFloodStageWatermark, frozenFloodStageMaxHeadroom, false);
    }

    private ByteSizeValue getMinimumTotalSizeForBelowWatermark(
        ByteSizeValue used,
        RelativeByteSizeValue watermark,
        ByteSizeValue maxHeadroom
    ) {
        // If watermark is absolute, simply return total disk = used disk + free disk, where free disk bytes is the watermark value.
        if (watermark.isAbsolute()) {
            return ByteSizeValue.ofBytes(watermark.getAbsolute().getBytes() + used.getBytes());
        }

        // If watermark is percentage/ratio, calculate the total needed disk space.
        // This may not be the minimum, due to the possible max headroom value which can cap the free disk space required.
        double ratioThreshold = watermark.getRatio().getAsRatio();
        if (ratioThreshold >= 0.0 && ratioThreshold < 1.0) {
            ByteSizeValue totalBytes = ByteSizeValue.ofBytes((long) Math.ceil(used.getBytes() / ratioThreshold));

            // Now calculate the minimum free bytes, taking into account the possible max headroom value as well.
            ByteSizeValue minimumFreeBytes = getFreeBytesThreshold(totalBytes, watermark, maxHeadroom, true);

            // Finally return used + minimum free bytes
            return ByteSizeValue.ofBytes(minimumFreeBytes.getBytes() + used.getBytes());
        } else {
            return used;
        }
    }

    public ByteSizeValue getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue used) {
        return getMinimumTotalSizeForBelowWatermark(used, lowStageWatermark, lowStageMaxHeadroom);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public TimeValue getRerouteInterval() {
        return rerouteInterval;
    }

    private String describeThreshold(
        ByteSizeValue total,
        RelativeByteSizeValue watermark,
        ByteSizeValue maxHeadroom,
        boolean includeSettingKey,
        String watermarkSettingKey,
        String maxHeadroomSettingKey
    ) {
        if (watermark.isAbsolute()) {
            return includeSettingKey ? watermarkSettingKey + "=" + watermark.getStringRep() : watermark.getStringRep();
        } else if (watermark.calculateValue(total, maxHeadroom, false).equals(watermark.calculateValue(total, null, false))) {
            String value = watermark.getStringRep();
            return includeSettingKey ? watermarkSettingKey + "=" + value : value;
        } else {
            return includeSettingKey
                ? maxHeadroomSettingKey + "=" + maxHeadroom.getStringRep()
                : "max_headroom=" + maxHeadroom.getStringRep();
        }
    }

    public String describeLowThreshold(ByteSizeValue total, boolean includeSettingKey) {
        return describeThreshold(
            total,
            lowStageWatermark,
            lowStageMaxHeadroom,
            includeSettingKey,
            CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
            CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey()
        );
    }

    public String describeHighThreshold(ByteSizeValue total, boolean includeSettingKey) {
        return describeThreshold(
            total,
            highStageWatermark,
            highStageMaxHeadroom,
            includeSettingKey,
            CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
            CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey()
        );
    }

    public String describeFloodStageThreshold(ByteSizeValue total, boolean includeSettingKey) {
        return describeThreshold(
            total,
            floodStageWatermark,
            floodStageMaxHeadroom,
            includeSettingKey,
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey()
        );
    }

    public String describeFrozenFloodStageThreshold(ByteSizeValue total, boolean includeSettingKey) {
        return describeThreshold(
            total,
            frozenFloodStageWatermark,
            frozenFloodStageMaxHeadroom,
            includeSettingKey,
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey(),
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.getKey()
        );
    }

}
