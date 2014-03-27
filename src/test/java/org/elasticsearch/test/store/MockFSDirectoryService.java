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

package org.elasticsearch.test.store;

import com.google.common.base.Charsets;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockFactory;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.IndexShardException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.fs.FsDirectoryService;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ImmutableTestCluster;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Random;

public class MockFSDirectoryService extends FsDirectoryService {

    private final MockDirectoryHelper helper;
    private FsDirectoryService delegateService;
    public static final String CHECK_INDEX_ON_CLOSE = "index.store.mock.check_index_on_close";
    private final boolean checkIndexOnClose;

    @Inject
    public MockFSDirectoryService(final ShardId shardId, @IndexSettings Settings indexSettings, IndexStore indexStore, final IndicesService service) {
        super(shardId, indexSettings, indexStore);
        final long seed = indexSettings.getAsLong(ImmutableTestCluster.SETTING_INDEX_SEED, 0l);
        Random random = new Random(seed);
        helper = new MockDirectoryHelper(shardId, indexSettings, logger, random, seed);
        checkIndexOnClose = indexSettings.getAsBoolean(CHECK_INDEX_ON_CLOSE, random.nextDouble() < 0.1);

        delegateService = helper.randomDirectorService(indexStore);
        if (checkIndexOnClose) {
            final IndicesLifecycle.Listener listener = new IndicesLifecycle.Listener() {
                @Override
                public void beforeIndexShardClosed(ShardId sid, @Nullable IndexShard indexShard) {
                    if (shardId.equals(sid) && indexShard != null) {
                        checkIndex(((InternalIndexShard) indexShard).store());
                    }
                    service.indicesLifecycle().removeListener(this);
                }
            };
            service.indicesLifecycle().addListener(listener);
        }
    }

    @Override
    public Directory[] build() throws IOException {
        return helper.wrapAllInplace(delegateService.build());
    }
    
    @Override
    protected synchronized FSDirectory newFSDirectory(File location, LockFactory lockFactory) throws IOException {
        throw new UnsupportedOperationException();
    }


    private void checkIndex(Store store) throws IndexShardException {
        try {
            if (!Lucene.indexExists(store.directory())) {
                return;
            }
            CheckIndex checkIndex = new CheckIndex(store.directory());
            BytesStreamOutput os = new BytesStreamOutput();
            PrintStream out = new PrintStream(os, false, Charsets.UTF_8.name());
            checkIndex.setInfoStream(out);
            out.flush();
            CheckIndex.Status status = checkIndex.checkIndex();
            if (!status.clean) {
                logger.warn("check index [failure]\n{}", new String(os.bytes().toBytes(), Charsets.UTF_8));
                throw new IndexShardException(shardId, "index check failure");
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("check index [success]\n{}", new String(os.bytes().toBytes(), Charsets.UTF_8));
                }
            }
        } catch (Exception e) {
            logger.warn("failed to check index", e);
        }
    }
}
