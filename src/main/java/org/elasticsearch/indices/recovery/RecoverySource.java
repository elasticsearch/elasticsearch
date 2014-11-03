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

package org.elasticsearch.indices.recovery;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateNonMasterUpdateTask;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The source recovery accepts recovery requests from other peer shards and start the recovery process from this
 * source shard to the target shard.
 */
public class RecoverySource extends AbstractComponent {

    public static class Actions {
        public static final String START_RECOVERY = "internal:index/shard/recovery/start_recovery";
    }

    private final TransportService transportService;
    private final IndicesService indicesService;
    private final RecoverySettings recoverySettings;
    private final MappingUpdatedAction mappingUpdatedAction;

    private final ClusterService clusterService;

    private final TimeValue internalActionTimeout;
    private final TimeValue internalActionLongTimeout;


    @Inject
    public RecoverySource(Settings settings, TransportService transportService, IndicesService indicesService,
                          RecoverySettings recoverySettings, MappingUpdatedAction mappingUpdatedAction, ClusterService clusterService) {
        super(settings);
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.clusterService = clusterService;

        this.recoverySettings = recoverySettings;

        transportService.registerHandler(Actions.START_RECOVERY, new StartRecoveryTransportRequestHandler());
        this.internalActionTimeout = componentSettings.getAsTime("internal_action_timeout", TimeValue.timeValueMinutes(15));
        this.internalActionLongTimeout = new TimeValue(internalActionTimeout.millis() * 2);
    }

    private RecoveryResponse recover(final StartRecoveryRequest request) {
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().index().name());
        final InternalIndexShard shard = (InternalIndexShard) indexService.shardSafe(request.shardId().id());

        // starting recovery from that our (the source) shard state is marking the shard to be in recovery mode as well, otherwise
        // the index operations will not be routed to it properly
        RoutingNode node = clusterService.state().readOnlyRoutingNodes().node(request.targetNode().id());
        if (node == null) {
            logger.debug("delaying recovery of {} as source node {} is unknown", request.shardId(), request.targetNode());
            throw new DelayRecoveryException("source node does not have the node [" + request.targetNode() + "] in its state yet..");
        }
        ShardRouting targetShardRouting = null;
        for (ShardRouting shardRouting : node) {
            if (shardRouting.shardId().equals(request.shardId())) {
                targetShardRouting = shardRouting;
                break;
            }
        }
        if (targetShardRouting == null) {
            logger.debug("delaying recovery of {} as it is not listed as assigned to target node {}", request.shardId(), request.targetNode());
            throw new DelayRecoveryException("source node does not have the shard listed in its state as allocated on the node");
        }
        if (!targetShardRouting.initializing()) {
            logger.debug("delaying recovery of {} as it is not listed as initializing on the target node {}. known shards state is [{}]",
                    request.shardId(), request.targetNode(), targetShardRouting.state());
            throw new DelayRecoveryException("source node has the state of the target shard to be [" + targetShardRouting.state() + "], expecting to be [initializing]");
        }

        logger.trace("[{}][{}] starting recovery to {}, mark_as_relocated {}", request.shardId().index().name(), request.shardId().id(), request.targetNode(), request.markAsRelocated());
        final RecoveryResponse response = new RecoveryResponse();
        shard.recover(new Engine.RecoveryHandler() {
            @Override
            public void phase1(final SnapshotIndexCommit snapshot) throws ElasticsearchException {
                long totalSize = 0;
                long existingTotalSize = 0;
                final Store store = shard.store();
                store.incRef();
                try {
                    StopWatch stopWatch = new StopWatch().start();
                    final Store.MetadataSnapshot recoverySourceMetadata = store.getMetadata(snapshot);
                    for (String name : snapshot.getFiles()) {
                        final StoreFileMetaData md = recoverySourceMetadata.get(name);
                        if (md == null) {
                            logger.info("Snapshot differs from actual index for file: {} meta: {}", name, recoverySourceMetadata.asMap());
                            throw new CorruptIndexException("Snapshot differs from actual index - maybe index was removed metadata has " + recoverySourceMetadata.asMap().size() + " files");
                        }
                    }
                    final Store.RecoveryDiff diff = recoverySourceMetadata.recoveryDiff(new Store.MetadataSnapshot(request.existingFiles()));
                    for (StoreFileMetaData md : diff.identical) {
                        response.phase1ExistingFileNames.add(md.name());
                        response.phase1ExistingFileSizes.add(md.length());
                        existingTotalSize += md.length();
                        if (logger.isTraceEnabled()) {
                            logger.trace("[{}][{}] recovery [phase1] to {}: not recovering [{}], exists in local store and has checksum [{}], size [{}]", request.shardId().index().name(), request.shardId().id(), request.targetNode(), md.name(), md.checksum(), md.length());
                        }
                        totalSize += md.length();
                    }
                    for (StoreFileMetaData md : Iterables.concat(diff.different, diff.missing)) {
                        if (request.existingFiles().containsKey(md.name())) {
                            logger.trace("[{}][{}] recovery [phase1] to {}: recovering [{}], exists in local store, but is different: remote [{}], local [{}]", request.shardId().index().name(), request.shardId().id(), request.targetNode(), md.name(), request.existingFiles().get(md.name()), md);
                        } else {
                            logger.trace("[{}][{}] recovery [phase1] to {}: recovering [{}], does not exists in remote", request.shardId().index().name(), request.shardId().id(), request.targetNode(), md.name());
                        }
                        response.phase1FileNames.add(md.name());
                        response.phase1FileSizes.add(md.length());
                        totalSize += md.length();
                    }
                    response.phase1TotalSize = totalSize;
                    response.phase1ExistingTotalSize = existingTotalSize;

                    logger.trace("[{}][{}] recovery [phase1] to {}: recovering_files [{}] with total_size [{}], reusing_files [{}] with total_size [{}]", request.shardId().index().name(), request.shardId().id(), request.targetNode(), response.phase1FileNames.size(), new ByteSizeValue(totalSize), response.phase1ExistingFileNames.size(), new ByteSizeValue(existingTotalSize));

                    RecoveryFilesInfoRequest recoveryInfoFilesRequest = new RecoveryFilesInfoRequest(request.recoveryId(), request.shardId(), response.phase1FileNames, response.phase1FileSizes,
                            response.phase1ExistingFileNames, response.phase1ExistingFileSizes, response.phase1TotalSize, response.phase1ExistingTotalSize);
                    transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.FILES_INFO, recoveryInfoFilesRequest, TransportRequestOptions.options().withTimeout(internalActionTimeout), EmptyTransportResponseHandler.INSTANCE_SAME).txGet();

                    final CountDownLatch latch = new CountDownLatch(response.phase1FileNames.size());
                    final CopyOnWriteArrayList<Throwable> exceptions = new CopyOnWriteArrayList<>();
                    final AtomicReference<CorruptIndexException> corruptedEngine = new AtomicReference<>();
                    int fileIndex = 0;
                    for (final String name : response.phase1FileNames) {
                        ThreadPoolExecutor pool;
                        long fileSize = response.phase1FileSizes.get(fileIndex);
                        if (fileSize > recoverySettings.SMALL_FILE_CUTOFF_BYTES) {
                            pool = recoverySettings.concurrentStreamPool();
                        } else {
                            pool = recoverySettings.concurrentSmallFileStreamPool();
                        }

                        pool.execute(new Runnable() {
                            @Override
                            public void run() {
                                store.incRef();
                                final StoreFileMetaData md = recoverySourceMetadata.get(name);
                                try (final IndexInput indexInput = store.directory().openInput(name, IOContext.READONCE)) {
                                    final int BUFFER_SIZE = (int) recoverySettings.fileChunkSize().bytes();
                                    final byte[] buf = new byte[BUFFER_SIZE];
                                    boolean shouldCompressRequest = recoverySettings.compress();
                                    if (CompressorFactory.isCompressed(indexInput)) {
                                        shouldCompressRequest = false;
                                    }

                                    long len = indexInput.length();
                                    long readCount = 0;
                                    while (readCount < len) {
                                        if (shard.state() == IndexShardState.CLOSED) { // check if the shard got closed on us
                                            throw new IndexShardClosedException(shard.shardId());
                                        }
                                        int toRead = readCount + BUFFER_SIZE > len ? (int) (len - readCount) : BUFFER_SIZE;
                                        long position = indexInput.getFilePointer();

                                        if (recoverySettings.rateLimiter() != null) {
                                            recoverySettings.rateLimiter().pause(toRead);
                                        }

                                        indexInput.readBytes(buf, 0, toRead, false);
                                        BytesArray content = new BytesArray(buf, 0, toRead);
                                        readCount += toRead;
                                        transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.FILE_CHUNK, new RecoveryFileChunkRequest(request.recoveryId(), request.shardId(), md, position, content, readCount == len),
                                                TransportRequestOptions.options().withCompress(shouldCompressRequest).withType(TransportRequestOptions.Type.RECOVERY).withTimeout(internalActionTimeout), EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                                    }
                                } catch (Throwable e) {
                                    final CorruptIndexException corruptIndexException;
                                    if ((corruptIndexException = ExceptionsHelper.unwrap(e, CorruptIndexException.class)) != null) {
                                       if (store.checkIntegrity(md) == false) { // we are corrupted on the primary -- fail!
                                           logger.warn("{} Corrupted file detected {} checksum mismatch", shard.shardId(), md);
                                           CorruptIndexException current = corruptedEngine.get();
                                           if (current != null || corruptedEngine.compareAndSet(null, corruptIndexException)) {
                                               if (current == null) {
                                                   current = corruptedEngine.get();
                                                   // important, we "unwrapped" current, which may be the root cause of e,
                                                   // so we can't add e as a suppressed exception, or it creates a cycle
                                               } else {
                                                   current.addSuppressed(e);
                                               }
                                           }

                                       } else { // corruption has happened on the way to replica
                                           RemoteTransportException exception = new RemoteTransportException("File corruption occured on recovery but checksums are ok", null);
                                           exception.addSuppressed(e);
                                           exceptions.add(0, exception); // last exception first
                                           logger.warn("{} File corruption on recovery {} local checksum OK", corruptIndexException, shard.shardId(), md);
                                       }
                                    } else {
                                        exceptions.add(0, e); // last exceptions first
                                    }
                                } finally {
                                    try {
                                        store.decRef();
                                    } finally {
                                        latch.countDown();
                                    }
                                }
                            }
                        });
                        fileIndex++;
                    }

                    latch.await();
                    if (corruptedEngine.get() != null) {
                        throw corruptedEngine.get();
                    } else {
                        ExceptionsHelper.rethrowAndSuppress(exceptions);
                    }

                    // now, set the clean files request
                    Set<String> snapshotFiles = Sets.newHashSet(snapshot.getFiles());
                    transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.CLEAN_FILES, new RecoveryCleanFilesRequest(request.recoveryId(), shard.shardId(), snapshotFiles), TransportRequestOptions.options().withTimeout(internalActionTimeout), EmptyTransportResponseHandler.INSTANCE_SAME).txGet();

                    stopWatch.stop();
                    logger.trace("[{}][{}] recovery [phase1] to {}: took [{}]", request.shardId().index().name(), request.shardId().id(), request.targetNode(), stopWatch.totalTime());
                    response.phase1Time = stopWatch.totalTime().millis();
                } catch (Throwable e) {
                    throw new RecoverFilesRecoveryException(request.shardId(), response.phase1FileNames.size(), new ByteSizeValue(totalSize), e);
                } finally {
                    store.decRef();
                }
            }

            @Override
            public void phase2(Translog.Snapshot snapshot) throws ElasticsearchException {
                if (shard.state() == IndexShardState.CLOSED) {
                    throw new IndexShardClosedException(request.shardId());
                }
                logger.trace("{} recovery [phase2] to {}: start", request.shardId(), request.targetNode());
                StopWatch stopWatch = new StopWatch().start();
                transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.PREPARE_TRANSLOG, new RecoveryPrepareForTranslogOperationsRequest(request.recoveryId(), request.shardId()), TransportRequestOptions.options().withTimeout(internalActionTimeout), EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                stopWatch.stop();
                response.startTime = stopWatch.totalTime().millis();
                logger.trace("{} recovery [phase2] to {}: start took [{}]", request.shardId(), request.targetNode(), request.targetNode(), stopWatch.totalTime());


                logger.trace("{} recovery [phase2] to {}: updating current mapping to master", request.shardId(), request.targetNode());
                updateMappingOnMaster();

                logger.trace("{} recovery [phase2] to {}: sending transaction log operations", request.shardId(), request.targetNode());
                stopWatch = new StopWatch().start();
                int totalOperations = sendSnapshot(snapshot);
                stopWatch.stop();
                logger.trace("{} recovery [phase2] to {}: took [{}]", request.shardId(), request.targetNode(), stopWatch.totalTime());
                response.phase2Time = stopWatch.totalTime().millis();
                response.phase2Operations = totalOperations;
            }

            private void updateMappingOnMaster() {
                // we test that the cluster state is in sync with our in memory mapping stored by the mapperService
                // we have to do it under the "cluster state update" thread to make sure that one doesn't modify it
                // while we're checking
                final BlockingQueue<DocumentMapper> documentMappersToUpdate = ConcurrentCollections.newBlockingQueue();
                final CountDownLatch latch = new CountDownLatch(1);
                clusterService.submitStateUpdateTask("recovery_mapping_check", new ProcessedClusterStateNonMasterUpdateTask() {
                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        latch.countDown();
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        IndexMetaData indexMetaData = clusterService.state().metaData().getIndices().get(indexService.index().getName());
                        ImmutableOpenMap<String, MappingMetaData> metaDataMappings = null;
                        if (indexMetaData != null) {
                            metaDataMappings = indexMetaData.getMappings();
                        }
                        // default mapping should not be sent back, it can only be updated by put mapping API, and its
                        // a full in place replace, we don't want to override a potential update coming it
                        for (DocumentMapper documentMapper : indexService.mapperService().docMappers(false)) {

                            MappingMetaData mappingMetaData = metaDataMappings == null ? null : metaDataMappings.get(documentMapper.type());
                            if (mappingMetaData == null || !documentMapper.refreshSource().equals(mappingMetaData.source())) {
                                // not on master yet in the right form
                                documentMappersToUpdate.add(documentMapper);
                            }
                        }
                        return currentState;
                    }

                    @Override
                    public void onFailure(String source, @Nullable Throwable t) {
                        logger.error("unexpected error while checking for pending mapping changes", t);
                        latch.countDown();
                    }
                });
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (documentMappersToUpdate.isEmpty()) {
                    return;
                }
                final CountDownLatch updatedOnMaster = new CountDownLatch(documentMappersToUpdate.size());
                MappingUpdatedAction.MappingUpdateListener listener = new MappingUpdatedAction.MappingUpdateListener() {
                    @Override
                    public void onMappingUpdate() {
                        updatedOnMaster.countDown();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        logger.debug("{} recovery to {}: failed to update mapping on master", request.shardId(), request.targetNode(), t);
                        updatedOnMaster.countDown();
                    }
                };
                for (DocumentMapper documentMapper : documentMappersToUpdate) {
                    mappingUpdatedAction.updateMappingOnMaster(indexService.index().getName(), documentMapper, indexService.indexUUID(), listener);
                }
                try {
                    if (!updatedOnMaster.await(internalActionTimeout.millis(), TimeUnit.MILLISECONDS)) {
                        logger.debug("{} recovery [phase2] to {}: waiting on pending mapping update timed out. waited [{}]", request.shardId(), request.targetNode(), internalActionTimeout);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.debug("interrupted while waiting for mapping to update on master");
                }

            }

            @Override
            public void phase3(Translog.Snapshot snapshot) throws ElasticsearchException {

                if (shard.state() == IndexShardState.CLOSED) {
                    throw new IndexShardClosedException(request.shardId());
                }
                logger.trace("[{}][{}] recovery [phase3] to {}: sending transaction log operations", request.shardId().index().name(), request.shardId().id(), request.targetNode());
                StopWatch stopWatch = new StopWatch().start();
                int totalOperations = sendSnapshot(snapshot);
                transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.FINALIZE, new RecoveryFinalizeRecoveryRequest(request.recoveryId(), request.shardId()), TransportRequestOptions.options().withTimeout(internalActionLongTimeout), EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                if (request.markAsRelocated()) {
                    // TODO what happens if the recovery process fails afterwards, we need to mark this back to started
                    try {
                        shard.relocated("to " + request.targetNode());
                    } catch (IllegalIndexShardStateException e) {
                        // we can ignore this exception since, on the other node, when it moved to phase3
                        // it will also send shard started, which might cause the index shard we work against
                        // to move be closed by the time we get to the the relocated method
                    }
                }
                stopWatch.stop();
                logger.trace("[{}][{}] recovery [phase3] to {}: took [{}]", request.shardId().index().name(), request.shardId().id(), request.targetNode(), stopWatch.totalTime());
                response.phase3Time = stopWatch.totalTime().millis();
                response.phase3Operations = totalOperations;
            }

            private int sendSnapshot(Translog.Snapshot snapshot) throws ElasticsearchException {
                int ops = 0;
                long size = 0;
                int totalOperations = 0;
                List<Translog.Operation> operations = Lists.newArrayList();
                Translog.Operation operation = snapshot.next();
                while (operation != null) {
                    if (shard.state() == IndexShardState.CLOSED) {
                        throw new IndexShardClosedException(request.shardId());
                    }

                    operations.add(operation);
                    ops += 1;
                    size += operation.estimateSize();
                    totalOperations++;
                    if (ops >= recoverySettings.translogOps() || size >= recoverySettings.translogSize().bytes()) {

                        // don't throttle translog, since we lock for phase3 indexing, so we need to move it as
                        // fast as possible. Note, sine we index docs to replicas while the index files are recovered
                        // the lock can potentially be removed, in which case, it might make sense to re-enable
                        // throttling in this phase
//                        if (recoverySettings.rateLimiter() != null) {
//                            recoverySettings.rateLimiter().pause(size);
//                        }

                        RecoveryTranslogOperationsRequest translogOperationsRequest = new RecoveryTranslogOperationsRequest(request.recoveryId(), request.shardId(), operations);
                        transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.TRANSLOG_OPS, translogOperationsRequest, TransportRequestOptions.options().withCompress(recoverySettings.compress()).withType(TransportRequestOptions.Type.RECOVERY).withTimeout(internalActionLongTimeout), EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                        ops = 0;
                        size = 0;
                        operations.clear();
                    }
                    operation = snapshot.next();
                }
                // send the leftover
                if (!operations.isEmpty()) {
                    RecoveryTranslogOperationsRequest translogOperationsRequest = new RecoveryTranslogOperationsRequest(request.recoveryId(), request.shardId(), operations);
                    transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.TRANSLOG_OPS, translogOperationsRequest, TransportRequestOptions.options().withCompress(recoverySettings.compress()).withType(TransportRequestOptions.Type.RECOVERY).withTimeout(internalActionLongTimeout), EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                }
                return totalOperations;
            }
        });
        return response;
    }

    class StartRecoveryTransportRequestHandler extends BaseTransportRequestHandler<StartRecoveryRequest> {

        @Override
        public StartRecoveryRequest newInstance() {
            return new StartRecoveryRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public void messageReceived(final StartRecoveryRequest request, final TransportChannel channel) throws Exception {
            RecoveryResponse response = recover(request);
            channel.sendResponse(response);
        }
    }
}

