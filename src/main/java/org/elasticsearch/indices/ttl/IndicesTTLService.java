/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.ttl;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fieldvisitor.UidAndRoutingFieldsVisitor;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A node level service that delete expired docs on node primary shards.
 */
public class IndicesTTLService extends AbstractLifecycleComponent<IndicesTTLService> {

    public static final String INDICES_TTL_INTERVAL = "indices.ttl.interval";
    public static final String INDEX_TTL_DISABLE_PURGE = "index.ttl.disable_purge";

    private static final TimeValue DEFAULT_TTL_INTERVAL = TimeValue.timeValueSeconds(60);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final TransportBulkAction bulkAction;

    private final int bulkSize;
    private PurgerThread purgerThread;

    @Inject
    public IndicesTTLService(Settings settings, ClusterService clusterService, IndicesService indicesService, NodeSettingsService nodeSettingsService, TransportBulkAction bulkAction) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        TimeValue interval = componentSettings.getAsTime("interval", DEFAULT_TTL_INTERVAL);
        this.bulkAction = bulkAction;
        this.bulkSize = componentSettings.getAsInt("bulk_size", 10000);
        this.purgerThread = new PurgerThread(EsExecutors.threadName(settings, "[ttl_expire]"), interval);

        nodeSettingsService.addListener(new ApplySettings());
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        this.purgerThread.start();
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        try {
            this.purgerThread.shutdown();
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    private class PurgerThread extends Thread {
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final Notifier notifier;
        private final CountDownLatch shutdownLatch = new CountDownLatch(1);


        public PurgerThread(String name, TimeValue interval) {
            super(name);
            setDaemon(true);
            this.notifier = new Notifier(interval);
        }

        public void shutdown() throws InterruptedException {
            if (running.compareAndSet(true, false)) {
                notifier.doNotify();
                shutdownLatch.await();
            }

        }

        public void resetInterval(TimeValue interval) {
            notifier.setTimeout(interval);
        }

        public void run() {
            try {
                while (running.get()) {
                    try {
                        List<IndexShard> shardsToPurge = getShardsToPurge();
                        purgeShards(shardsToPurge);
                    } catch (Throwable e) {
                        if (running.get()) {
                            logger.warn("failed to execute ttl purge", e);
                        }
                    }
                    if (running.get()) {
                        notifier.await();
                    }
                }
            } finally {
                shutdownLatch.countDown();
            }
        }

        /**
         * Returns the shards to purge, i.e. the local started primary shards that have ttl enabled and disable_purge to false
         */
        private List<IndexShard> getShardsToPurge() {
            List<IndexShard> shardsToPurge = new ArrayList<>();
            MetaData metaData = clusterService.state().metaData();
            for (IndexService indexService : indicesService) {
                // check the value of disable_purge for this index
                IndexMetaData indexMetaData = metaData.index(indexService.index().name());
                if (indexMetaData == null) {
                    continue;
                }
                boolean disablePurge = indexMetaData.settings().getAsBoolean(INDEX_TTL_DISABLE_PURGE, false);
                if (disablePurge) {
                    continue;
                }

                // should be optimized with the hasTTL flag
                FieldMappers ttlFieldMappers = indexService.mapperService().name(TTLFieldMapper.NAME);
                if (ttlFieldMappers == null) {
                    continue;
                }
                // check if ttl is enabled for at least one type of this index
                boolean hasTTLEnabled = false;
                for (FieldMapper ttlFieldMapper : ttlFieldMappers) {
                    if (((TTLFieldMapper) ttlFieldMapper).enabled()) {
                        hasTTLEnabled = true;
                        break;
                    }
                }
                if (hasTTLEnabled) {
                    for (IndexShard indexShard : indexService) {
                        if (indexShard.state() == IndexShardState.STARTED && indexShard.routingEntry().primary() && indexShard.routingEntry().started()) {
                            shardsToPurge.add(indexShard);
                        }
                    }
                }
            }
            return shardsToPurge;
        }

        public TimeValue getInterval() {
            return notifier.getTimeout();
        }
    }

    private void purgeShards(List<IndexShard> shardsToPurge) {
        for (IndexShard shardToPurge : shardsToPurge) {
            Query query = shardToPurge.indexService().mapperService().smartNameFieldMapper(TTLFieldMapper.NAME).rangeQuery(null, System.currentTimeMillis(), false, true, null);
            Engine.Searcher searcher = shardToPurge.acquireSearcher("indices_ttl");
            try {
                logger.debug("[{}][{}] purging shard", shardToPurge.routingEntry().index(), shardToPurge.routingEntry().id());
                ExpiredDocsCollector expiredDocsCollector = new ExpiredDocsCollector();
                searcher.searcher().search(query, expiredDocsCollector);
                List<DocToPurge> docsToPurge = expiredDocsCollector.getDocsToPurge();

                BulkRequest bulkRequest = new BulkRequest();
                for (DocToPurge docToPurge : docsToPurge) {

                    bulkRequest.add(new DeleteRequest().index(shardToPurge.routingEntry().index()).type(docToPurge.type).id(docToPurge.id).version(docToPurge.version).routing(docToPurge.routing));
                    bulkRequest = processBulkIfNeeded(bulkRequest, false);
                }
                processBulkIfNeeded(bulkRequest, true);
            } catch (Exception e) {
                logger.warn("failed to purge", e);
            } finally {
                searcher.close();
            }
        }
    }

    private static class DocToPurge {
        public final String type;
        public final String id;
        public final long version;
        public final String routing;

        public DocToPurge(String type, String id, long version, String routing) {
            this.type = type;
            this.id = id;
            this.version = version;
            this.routing = routing;
        }
    }

    private class ExpiredDocsCollector extends SimpleCollector {
        private LeafReaderContext context;
        private List<DocToPurge> docsToPurge = new ArrayList<>();

        public ExpiredDocsCollector() {
        }

        public void setScorer(Scorer scorer) {
        }

        public boolean acceptsDocsOutOfOrder() {
            return true;
        }

        public void collect(int doc) {
            try {
                UidAndRoutingFieldsVisitor fieldsVisitor = new UidAndRoutingFieldsVisitor();
                context.reader().document(doc, fieldsVisitor);
                Uid uid = fieldsVisitor.uid();
                final long version = Versions.loadVersion(context.reader(), new Term(UidFieldMapper.NAME, uid.toBytesRef()));
                docsToPurge.add(new DocToPurge(uid.type(), uid.id(), version, fieldsVisitor.routing()));
            } catch (Exception e) {
                logger.trace("failed to collect doc", e);
            }
        }

        public void doSetNextReader(LeafReaderContext context) throws IOException {
            this.context = context;
        }

        public List<DocToPurge> getDocsToPurge() {
            return this.docsToPurge;
        }
    }

    private BulkRequest processBulkIfNeeded(BulkRequest bulkRequest, boolean force) {
        if ((force && bulkRequest.numberOfActions() > 0) || bulkRequest.numberOfActions() >= bulkSize) {
            try {
                bulkAction.executeBulk(bulkRequest, new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse bulkResponse) {
                        logger.trace("bulk took " + bulkResponse.getTookInMillis() + "ms");
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        logger.warn("failed to execute bulk");
                    }
                });
            } catch (Exception e) {
                logger.warn("failed to process bulk", e);
            }
            bulkRequest = new BulkRequest();
        }
        return bulkRequest;
    }

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            final TimeValue currentInterval = IndicesTTLService.this.purgerThread.getInterval();
            final TimeValue interval = settings.getAsTime(INDICES_TTL_INTERVAL,
                    IndicesTTLService.this.settings.getAsTime(INDICES_TTL_INTERVAL, DEFAULT_TTL_INTERVAL));
            if (!interval.equals(currentInterval)) {
                logger.info("updating indices.ttl.interval from [{}] to [{}]",currentInterval, interval);
                IndicesTTLService.this.purgerThread.resetInterval(interval);

            }
        }
    }


    private static final class Notifier {

        private final ReentrantLock lock = new ReentrantLock();
        private final Condition condition = lock.newCondition();
        private volatile TimeValue timeout;

        public Notifier(TimeValue timeout) {
            assert timeout != null;
            this.timeout = timeout;
        }

        public void await() {
            lock.lock();
            try {
                condition.await(timeout.millis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.interrupted();
            } finally {
                lock.unlock();
            }

        }

        public void setTimeout(TimeValue timeout) {
            assert timeout != null;
            this.timeout = timeout;
            doNotify();
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        public void doNotify() {
            lock.lock();
            try {
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
}
