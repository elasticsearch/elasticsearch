/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.LicenseStateListener;
import org.elasticsearch.license.XPackLicenseState;

import java.util.HashSet;
import java.util.Set;

public class FailShardsOnInvalidLicenseClusterListener implements LicenseStateListener, IndexEventListener {

    private static final Logger logger = LogManager.getLogger(FailShardsOnInvalidLicenseClusterListener.class);

    private final XPackLicenseState xPackLicenseState;

    private final RerouteService rerouteService;

    final Set<IndexShard> shardsToFail = new HashSet<>();

    private boolean allowed;

    public FailShardsOnInvalidLicenseClusterListener(XPackLicenseState xPackLicenseState, RerouteService rerouteService) {
        this.xPackLicenseState = xPackLicenseState;
        this.rerouteService = rerouteService;
        this.allowed = xPackLicenseState.isAllowed(XPackLicenseState.Feature.SEARCHABLE_SNAPSHOTS);
        xPackLicenseState.addListener(this);
    }

    @Override
    public synchronized void afterIndexShardStarted(IndexShard indexShard) {
        shardsToFail.add(indexShard);
        failActiveShardsIfNecessary();
    }

    @Override
    public synchronized void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            shardsToFail.remove(indexShard);
        }
    }

    @Override
    public synchronized void licenseStateChanged() {
        final boolean allowed = xPackLicenseState.isAllowed(XPackLicenseState.Feature.SEARCHABLE_SNAPSHOTS);
        if (allowed && this.allowed == false) {
            rerouteService.reroute("reroute after license activation", Priority.NORMAL, new ActionListener<ClusterState>() {
                @Override
                public void onResponse(ClusterState clusterState) {
                    logger.trace("successful reroute after license activation");
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug("unsuccessful reroute after license activation");
                }
            });
        }
        this.allowed = allowed;
        failActiveShardsIfNecessary();
    }

    private void failActiveShardsIfNecessary() {
        assert Thread.holdsLock(this);
        if (allowed == false) {
            for (IndexShard indexShard : shardsToFail) {
                try {
                    indexShard.failShard("invalid license", null);
                } catch (AlreadyClosedException ignored) {
                    // ignore
                } catch (Exception e) {
                    logger.warn(new ParameterizedMessage("Could not close shard {} due to invalid license", indexShard.shardId()), e);
                }
            }
            shardsToFail.clear();
        }
    }
}
