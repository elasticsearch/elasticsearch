/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.cache.field.data.resident;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.base.Objects;
import org.elasticsearch.common.collect.MapEvictionListener;
import org.elasticsearch.common.collect.MapMaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.field.data.support.AbstractConcurrentMapFieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kimchy (shay.banon)
 */
public class ResidentFieldDataCache extends AbstractConcurrentMapFieldDataCache implements MapEvictionListener<String, FieldData> {

    private final IndexSettingsService indexSettingsService;

    private volatile int maxSize;
    private volatile TimeValue expire;

    private final AtomicLong evictions = new AtomicLong();

    private final ApplySettings applySettings = new ApplySettings();

    @Inject public ResidentFieldDataCache(Index index, @IndexSettings Settings indexSettings, IndexSettingsService indexSettingsService) {
        super(index, indexSettings);
        this.indexSettingsService = indexSettingsService;

        this.maxSize = indexSettings.getAsInt("index.cache.field.max_size", componentSettings.getAsInt("max_size", -1));
        this.expire = indexSettings.getAsTime("index.cache.field.expire", componentSettings.getAsTime("expire", null));
        logger.debug("using [resident] field cache with max_size [{}], expire [{}]", maxSize, expire);

        indexSettingsService.addListener(applySettings);
    }

    @Override public void close() throws ElasticSearchException {
        indexSettingsService.removeListener(applySettings);
        super.close();
    }

    @Override protected ConcurrentMap<String, FieldData> buildFieldDataMap() {
        MapMaker mapMaker = new MapMaker();
        if (maxSize != -1) {
            mapMaker.maximumSize(maxSize);
        }
        if (expire != null) {
            mapMaker.expireAfterAccess(expire.nanos(), TimeUnit.NANOSECONDS);
        }
        mapMaker.evictionListener(this);
        return mapMaker.makeMap();
    }

    @Override public String type() {
        return "resident";
    }

    @Override public long evictions() {
        return evictions.get();
    }

    @Override public void onEviction(@Nullable String s, @Nullable FieldData fieldData) {
        evictions.incrementAndGet();
    }

    class ApplySettings implements IndexSettingsService.Listener {
        @Override public void onRefreshSettings(Settings settings) {
            int maxSize = settings.getAsInt("index.cache.field.max_size", ResidentFieldDataCache.this.maxSize);
            TimeValue expire = settings.getAsTime("index.cache.field.expire", ResidentFieldDataCache.this.expire);
            boolean changed = false;
            if (maxSize != ResidentFieldDataCache.this.maxSize) {
                logger.info("updating index.cache.field.max_size from [{}] to [{}]", ResidentFieldDataCache.this.maxSize, maxSize);
                changed = true;
                ResidentFieldDataCache.this.maxSize = maxSize;
            }
            if (!Objects.equal(expire, ResidentFieldDataCache.this.expire)) {
                logger.info("updating index.cache.field.expire from [{}] to [{}]", ResidentFieldDataCache.this.expire, expire);
                changed = true;
                ResidentFieldDataCache.this.expire = expire;
            }
            if (changed) {
                clear();
            }
        }
    }
}
