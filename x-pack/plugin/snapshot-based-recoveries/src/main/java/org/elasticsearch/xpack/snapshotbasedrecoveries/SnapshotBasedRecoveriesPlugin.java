/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.snapshotbasedrecoveries;

import org.elasticsearch.indices.recovery.plan.RecoveryPlannerService;
import org.elasticsearch.indices.recovery.plan.ShardSnapshotsService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RecoveryPlannerPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.snapshotbasedrecoveries.recovery.plan.SnapshotsRecoveryPlannerService;

public class SnapshotBasedRecoveriesPlugin extends Plugin implements RecoveryPlannerPlugin {
    public static final LicensedFeature.Momentary SNAPSHOT_BASED_RECOVERIES_FEATURE = LicensedFeature.momentary(
        null,
        "snapshot-based-recoveries",
        License.OperationMode.ENTERPRISE
    );

    public SnapshotBasedRecoveriesPlugin() {}

    @Override
    public RecoveryPlannerService createRecoveryPlannerService(ShardSnapshotsService shardSnapshotsService) {
        return new SnapshotsRecoveryPlannerService(shardSnapshotsService, this::isLicenseEnabled);
    }

    // Overridable for tests
    public boolean isLicenseEnabled() {
        return SNAPSHOT_BASED_RECOVERIES_FEATURE.check(XPackPlugin.getSharedLicenseState());
    }
}
