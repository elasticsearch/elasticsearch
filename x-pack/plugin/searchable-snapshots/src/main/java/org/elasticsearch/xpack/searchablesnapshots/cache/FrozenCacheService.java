/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Assertions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.index.store.cache.SparseFileTracker;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;

public class FrozenCacheService implements Releasable {

    private static final String SETTINGS_PREFIX = "xpack.searchable.snapshot.shared_cache.";

    public static final Setting<ByteSizeValue> SNAPSHOT_CACHE_SIZE_SETTING = Setting.byteSizeSetting(
        SETTINGS_PREFIX + "size",
        ByteSizeValue.ZERO,
        Setting.Property.NodeScope
    );

    public static final ByteSizeValue MIN_SNAPSHOT_CACHE_RANGE_SIZE = new ByteSizeValue(4, ByteSizeUnit.KB);
    public static final ByteSizeValue MAX_SNAPSHOT_CACHE_RANGE_SIZE = new ByteSizeValue(Integer.MAX_VALUE, ByteSizeUnit.BYTES);

    public static final Setting<ByteSizeValue> FROZEN_CACHE_RANGE_SIZE_SETTING = Setting.byteSizeSetting(
        SETTINGS_PREFIX + "range_size",
        ByteSizeValue.ofMb(16),                                 // default
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> SNAPSHOT_CACHE_REGION_SIZE_SETTING = Setting.byteSizeSetting(
        SETTINGS_PREFIX + "region_size",
        FROZEN_CACHE_RANGE_SIZE_SETTING,
        Setting.Property.NodeScope
    );

    public static final Setting<Float> SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE = Setting.floatSetting(
        SETTINGS_PREFIX + "small_region_size_share",
        0.2f,
        0.0f,
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> FROZEN_CACHE_RECOVERY_RANGE_SIZE_SETTING = Setting.byteSizeSetting(
        SETTINGS_PREFIX + "recovery_range_size",
        new ByteSizeValue(128, ByteSizeUnit.KB),                // default
        MIN_SNAPSHOT_CACHE_RANGE_SIZE,                          // min
        MAX_SNAPSHOT_CACHE_RANGE_SIZE,                          // max
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> SNAPSHOT_CACHE_SMALL_REGION_SIZE = Setting.byteSizeSetting(
        SETTINGS_PREFIX + "small_region_size_",
        FROZEN_CACHE_RECOVERY_RANGE_SIZE_SETTING,
        Setting.Property.NodeScope
    );

    public static final TimeValue MIN_SNAPSHOT_CACHE_DECAY_INTERVAL = TimeValue.timeValueSeconds(1L);
    public static final Setting<TimeValue> SNAPSHOT_CACHE_DECAY_INTERVAL_SETTING = Setting.timeSetting(
        SETTINGS_PREFIX + "decay.interval",
        TimeValue.timeValueSeconds(60L),                        // default
        MIN_SNAPSHOT_CACHE_DECAY_INTERVAL,                      // min
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> SNAPSHOT_CACHE_MAX_FREQ_SETTING = Setting.intSetting(
        SETTINGS_PREFIX + "max_freq",
        100,                       // default
        1,                            // min
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> SNAPSHOT_CACHE_MIN_TIME_DELTA_SETTING = Setting.timeSetting(
        SETTINGS_PREFIX + "min_time_delta",
        TimeValue.timeValueSeconds(60L),                        // default
        TimeValue.timeValueSeconds(0L),                         // min
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(FrozenCacheService.class);

    private final ConcurrentHashMap<RegionKey, Entry<CacheFileRegion>> keyMapping;

    private final LongSupplier currentTimeSupplier;

    private final KeyedLock<CacheKey> keyedLock = new KeyedLock<>();

    private final SharedBytes sharedBytes;
    private final long regionSize;
    private final long smallRegionSize;
    private final ByteSizeValue rangeSize;
    private final ByteSizeValue recoveryRangeSize;

    private final ConcurrentLinkedQueue<Integer> freeRegions = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Integer> freeSmallRegions = new ConcurrentLinkedQueue<>();
    private final Entry<CacheFileRegion>[] freqs;
    private final int maxFreq;
    private final long minTimeDelta;

    private final AtomicReference<CacheFileRegion>[] regionOwners; // to assert exclusive access of regions

    private final CacheDecayTask decayTask;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public FrozenCacheService(Environment environment, ThreadPool threadPool) {
        this.currentTimeSupplier = threadPool::relativeTimeInMillis;
        final Settings settings = environment.settings();
        final long cacheSize = SNAPSHOT_CACHE_SIZE_SETTING.get(settings).getBytes();
        final long regionSize = SNAPSHOT_CACHE_REGION_SIZE_SETTING.get(settings).getBytes();
        this.smallRegionSize = Math.min(SNAPSHOT_CACHE_SMALL_REGION_SIZE.get(settings).getBytes(), regionSize / 2);
        final float smallRegionShare = SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE.get(settings);
        final int numRegions = Math.round(Math.toIntExact(cacheSize / regionSize) * (1 - smallRegionShare));
        final int numSmallRegions = Math.round(Math.toIntExact(cacheSize / smallRegionSize) * smallRegionShare);
        keyMapping = new ConcurrentHashMap<>();
        if (Assertions.ENABLED) {
            regionOwners = new AtomicReference[numRegions + numSmallRegions];
            for (int i = 0; i < numRegions + numSmallRegions; i++) {
                regionOwners[i] = new AtomicReference<>();
            }
        } else {
            regionOwners = null;
        }
        for (int i = 0; i < numRegions; i++) {
            freeRegions.add(i);
        }
        for (int i = 0; i < numSmallRegions; i++) {
            freeSmallRegions.add(numRegions + i);
        }
        this.regionSize = regionSize;
        assert regionSize > 0L;
        this.maxFreq = SNAPSHOT_CACHE_MAX_FREQ_SETTING.get(settings);
        this.minTimeDelta = SNAPSHOT_CACHE_MIN_TIME_DELTA_SETTING.get(settings).millis();
        freqs = new Entry[maxFreq];
        try {
            sharedBytes = new SharedBytes(numRegions, regionSize, numSmallRegions, smallRegionSize, environment);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        decayTask = new CacheDecayTask(threadPool, SNAPSHOT_CACHE_DECAY_INTERVAL_SETTING.get(settings));
        decayTask.rescheduleIfNecessary();
        this.rangeSize = FROZEN_CACHE_RANGE_SIZE_SETTING.get(settings);
        this.recoveryRangeSize = FROZEN_CACHE_RECOVERY_RANGE_SIZE_SETTING.get(settings);
    }

    public int getRangeSize() {
        return toIntBytes(rangeSize.getBytes());
    }

    public int getRecoveryRangeSize() {
        return toIntBytes(recoveryRangeSize.getBytes());
    }

    // Number of large regions used when caching a file of the given length
    private int largeRegions(long fileLength) {
        final int largeRegions = Math.toIntExact(fileLength / regionSize);
        final long remainder = fileLength % regionSize;
        if (remainder == 0) {
            return largeRegions;
        }
        // if we fill up the next region more than 50%, add another region
        if (remainder > regionSize / 2) {
            return largeRegions + 1;
        }
        final int smallRegionsNeeded = Math.toIntExact(remainder / smallRegionSize);
        // arbitrary heuristic: don't create more than twice the value of (large regions + 1) to strike a balance between not wasting too
        // much space to fragmentation and not having to manage too many regions.
        // TODO: this would be nicer if we had a fixed or power of 2 ratio between the region sizes?
        if (smallRegionsNeeded <= 2 * (largeRegions + 1)) {
            return largeRegions;
        }
        // It would have taken too many small regions to cache the last partial large page so we just use another large one at the cost of
        // disk space over number of regions
        return largeRegions + 1;
    }

    // get the region of a file of the given size that the given position belongs to
    private int getRegion(long position, long fileSize) {
        final int numberOfLargeRegions = largeRegions(fileSize);
        final int largeRegionIndex = Math.toIntExact(position / regionSize);
        if (largeRegionIndex < numberOfLargeRegions) {
            return largeRegionIndex;
        }
        final long remainder = position % regionSize;
        return numberOfLargeRegions + Math.toIntExact(remainder / smallRegionSize) + (remainder > 0 && remainder % smallRegionSize == 0
            ? -1
            : 0);
    }

    // get the relative position from the start of its region for the given position in a file of given size
    private long getRegionRelativePosition(long position, long fileSize) {
        final int region = getRegion(position, fileSize);
        return isSmallRegion(region, fileSize) ? (position % regionSize) % smallRegionSize : position % regionSize;
    }

    // get the number of bytes between the beginning of a file of the given size and the start of the given region
    private long getRegionStart(int region, long fileSize) {
        final int largeRegions = largeRegions(fileSize);
        return isSmallRegion(region, fileSize)
            ? largeRegions * regionSize + (region - largeRegions) * smallRegionSize
            : region * regionSize;
    }

    private ByteRange getRegionRange(int region, long fileLength) {
        final long regionStart = getRegionStart(region, fileLength);
        return ByteRange.of(regionStart, regionStart + (isSmallRegion(region, fileLength) ? smallRegionSize : regionSize));
    }

    private ByteRange mapSubRangeToRegion(ByteRange range, int region, long fileLength) {
        final ByteRange regionRange = getRegionRange(region, fileLength);
        if (range.hasOverlap(regionRange) == false) {
            return ByteRange.EMPTY;
        }
        final ByteRange overlap = regionRange.overlap(range);
        return ByteRange.of(
            getRegionRelativePosition(overlap.start(), fileLength),
            overlap.end() == regionRange.end()
                ? (isSmallRegion(region, fileLength) ? smallRegionSize : regionSize)
                : getRegionRelativePosition(overlap.end(), fileLength)
        );
    }

    private long getRegionSize(long fileLength, int region) {
        final boolean small = isSmallRegion(region, fileLength);
        final long currentRegionSize = small ? smallRegionSize : this.regionSize;
        assert fileLength > 0;
        final int maxRegion = getRegion(fileLength, fileLength);
        assert region >= 0 && region <= maxRegion;
        final long effectiveRegionSize;
        final long regionStart = getRegionStart(region, fileLength);
        if (region == maxRegion && regionStart + currentRegionSize != fileLength) {
            assert getRegionRelativePosition(fileLength, fileLength) != 0L;
            effectiveRegionSize = getRegionRelativePosition(fileLength, fileLength);
        } else {
            effectiveRegionSize = currentRegionSize;
        }
        assert regionStart + effectiveRegionSize <= fileLength;
        return effectiveRegionSize;
    }

    private boolean isSmallRegion(int region, long fileSize) {
        return region >= largeRegions(fileSize);
    }

    public CacheFileRegion get(CacheKey cacheKey, long fileLength, int region) {
        final long regionSize = getRegionSize(fileLength, region);
        try (Releasable ignore = keyedLock.acquire(cacheKey)) {
            final RegionKey regionKey = new RegionKey(cacheKey, region);
            final long now = currentTimeSupplier.getAsLong();
            final Entry<CacheFileRegion> entry = keyMapping.computeIfAbsent(
                regionKey,
                key -> new Entry<>(new CacheFileRegion(regionKey, regionSize, fileLength), now)
            );
            if (entry.chunk.sharedBytesPos == -1) {
                // new item
                assert entry.freq == 0;
                assert entry.prev == null;
                assert entry.next == null;
                final boolean isSmall = isSmallRegion(region, fileLength);
                final Integer freeSlot = tryPollFreeSlot(isSmall);
                if (freeSlot != null) {
                    // no need to evict an item, just add
                    acquireSlotForEntry(entry, freeSlot);
                } else {
                    // need to evict something
                    synchronized (this) {
                        maybeEvict();
                    }
                    final Integer freeSlotRetry = tryPollFreeSlot(isSmall);
                    if (freeSlotRetry != null) {
                        acquireSlotForEntry(entry, freeSlotRetry);
                    } else {
                        boolean removed = keyMapping.remove(regionKey, entry);
                        assert removed;
                        throw new AlreadyClosedException("no free region found");
                    }
                }
            } else {
                // check if we need to promote item
                synchronized (this) {
                    if (now - entry.lastAccessed >= minTimeDelta && entry.freq + 1 < maxFreq) {
                        unlink(entry);
                        entry.freq++;
                        entry.lastAccessed = now;
                        pushEntryToBack(entry);
                    }
                }
            }
            return entry.chunk;
        }
    }

    private Integer tryPollFreeSlot(boolean isSmall) {
        return (isSmall ? freeSmallRegions : freeRegions).poll();
    }

    private void acquireSlotForEntry(Entry<CacheFileRegion> entry, int freeSlot) {
        entry.chunk.sharedBytesPos = freeSlot;
        assert regionOwners[freeSlot].compareAndSet(null, entry.chunk);
        synchronized (this) {
            pushEntryToBack(entry);
        }
    }

    public void onClose(CacheFileRegion chunk) {
        assert regionOwners[chunk.sharedBytesPos].compareAndSet(chunk, null);
        if (chunk.sharedBytesPos >= sharedBytes.numRegions) {
            freeSmallRegions.add(chunk.sharedBytesPos);
        } else {
            freeRegions.add(chunk.sharedBytesPos);
        }
    }

    // used by tests
    int freeRegionCount() {
        return freeRegions.size();
    }

    private synchronized boolean invariant(final Entry<CacheFileRegion> e, boolean present) {
        boolean found = false;
        for (int i = 0; i < maxFreq; i++) {
            assert freqs[i] == null || freqs[i].prev != null;
            assert freqs[i] == null || freqs[i].prev != freqs[i] || freqs[i].next == null;
            assert freqs[i] == null || freqs[i].prev.next == null;
            for (Entry<CacheFileRegion> entry = freqs[i]; entry != null; entry = entry.next) {
                assert entry.next == null || entry.next.prev == entry;
                assert entry.prev != null;
                assert entry.prev.next == null || entry.prev.next == entry;
                assert entry.freq == i;
                if (entry == e) {
                    found = true;
                }
            }
            for (Entry<CacheFileRegion> entry = freqs[i]; entry != null && entry.prev != freqs[i]; entry = entry.prev) {
                assert entry.next == null || entry.next.prev == entry;
                assert entry.prev != null;
                assert entry.prev.next == null || entry.prev.next == entry;
                assert entry.freq == i;
                if (entry == e) {
                    found = true;
                }
            }
        }
        assert found == present;
        return true;
    }

    private void maybeEvict() {
        assert Thread.holdsLock(this);
        for (int i = 0; i < maxFreq; i++) {
            for (Entry<CacheFileRegion> entry = freqs[i]; entry != null; entry = entry.next) {
                boolean evicted = entry.chunk.tryEvict();
                if (evicted) {
                    unlink(entry);
                    keyMapping.remove(entry.chunk.regionKey, entry);
                    return;
                }
            }
        }
    }

    private void pushEntryToBack(final Entry<CacheFileRegion> entry) {
        assert Thread.holdsLock(this);
        assert invariant(entry, false);
        assert entry.prev == null;
        assert entry.next == null;
        final Entry<CacheFileRegion> currFront = freqs[entry.freq];
        if (currFront == null) {
            freqs[entry.freq] = entry;
            entry.prev = entry;
            entry.next = null;
        } else {
            assert currFront.freq == entry.freq;
            final Entry<CacheFileRegion> last = currFront.prev;
            currFront.prev = entry;
            last.next = entry;
            entry.prev = last;
            entry.next = null;
        }
        assert freqs[entry.freq].prev == entry;
        assert freqs[entry.freq].prev.next == null;
        assert entry.prev != null;
        assert entry.prev.next == null || entry.prev.next == entry;
        assert entry.next == null;
        assert invariant(entry, true);
    }

    private void unlink(final Entry<CacheFileRegion> entry) {
        assert Thread.holdsLock(this);
        assert invariant(entry, true);
        assert entry.prev != null;
        final Entry<CacheFileRegion> currFront = freqs[entry.freq];
        assert currFront != null;
        if (currFront == entry) {
            freqs[entry.freq] = entry.next;
            if (entry.next != null) {
                assert entry.prev != entry;
                entry.next.prev = entry.prev;
            }
        } else {
            if (entry.next != null) {
                entry.next.prev = entry.prev;
            }
            entry.prev.next = entry.next;
            if (currFront.prev == entry) {
                currFront.prev = entry.prev;
            }
        }
        entry.next = null;
        entry.prev = null;
        assert invariant(entry, false);
    }

    private void computeDecay() {
        synchronized (this) {
            long now = currentTimeSupplier.getAsLong();
            for (int i = 0; i < maxFreq; i++) {
                for (Entry<CacheFileRegion> entry = freqs[i]; entry != null; entry = entry.next) {
                    if (now - entry.lastAccessed >= 2 * minTimeDelta) {
                        if (entry.freq > 0) {
                            unlink(entry);
                            entry.freq--;
                            pushEntryToBack(entry);
                        }
                    }
                }
            }
        }
    }

    public void removeFromCache(CacheKey cacheKey) {
        forceEvict(cacheKey::equals);
    }

    public void markShardAsEvictedInCache(String snapshotUUID, String snapshotIndexName, ShardId shardId) {
        forceEvict(
            k -> shardId.equals(k.getShardId())
                && snapshotIndexName.equals(k.getSnapshotIndexName())
                && snapshotUUID.equals(k.getSnapshotUUID())
        );
    }

    private void forceEvict(Predicate<CacheKey> cacheKeyPredicate) {
        final List<Entry<CacheFileRegion>> matchingEntries = new ArrayList<>();
        keyMapping.forEach((key, value) -> {
            if (cacheKeyPredicate.test(key.file)) {
                matchingEntries.add(value);
            }
        });
        if (matchingEntries.isEmpty() == false) {
            synchronized (this) {
                for (Entry<CacheFileRegion> entry : matchingEntries) {
                    boolean evicted = entry.chunk.forceEvict();
                    if (evicted) {
                        unlink(entry);
                        keyMapping.remove(entry.chunk.regionKey, entry);
                    }
                }
            }
        }
    }

    // used by tests
    int getFreq(CacheFileRegion cacheFileRegion) {
        return keyMapping.get(cacheFileRegion.regionKey).freq;
    }

    @Override
    public void close() {
        sharedBytes.decRef();
    }

    class CacheDecayTask extends AbstractAsyncTask {

        CacheDecayTask(ThreadPool threadPool, TimeValue interval) {
            super(logger, Objects.requireNonNull(threadPool), Objects.requireNonNull(interval), true);
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        public void runInternal() {
            computeDecay();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public String toString() {
            return "frozen_cache_decay_task";
        }
    }

    private static class RegionKey {
        RegionKey(CacheKey file, int region) {
            this.file = file;
            this.region = region;
        }

        final CacheKey file;
        final int region;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RegionKey regionKey = (RegionKey) o;
            return region == regionKey.region && file.equals(regionKey.file);
        }

        @Override
        public int hashCode() {
            return Objects.hash(file, region);
        }

        @Override
        public String toString() {
            return "Chunk{" + "file=" + file + ", region=" + region + '}';
        }
    }

    static class Entry<T> {
        final T chunk;
        Entry<T> prev;
        Entry<T> next;
        int freq;
        long lastAccessed;

        Entry(T chunk, long lastAccessed) {
            this.chunk = chunk;
            this.lastAccessed = lastAccessed;
        }
    }

    class CacheFileRegion extends AbstractRefCounted {
        final RegionKey regionKey;
        final long fileSize;
        final SparseFileTracker tracker;
        volatile int sharedBytesPos = -1;

        CacheFileRegion(RegionKey regionKey, long regionSize, long fileSize) {
            super("CacheFileRegion");
            this.regionKey = regionKey;
            assert regionSize > 0L;
            this.fileSize = fileSize;
            tracker = new SparseFileTracker("file", regionSize);
        }

        public long physicalStartOffset() {
            return sharedBytes.getPhysicalOffset(sharedBytesPos);
        }

        public long physicalEndOffset() {
            return sharedBytes.getPhysicalOffset(sharedBytesPos + 1);
        }

        // If true this file region has been evicted from the cache and should not be used any more
        private final AtomicBoolean evicted = new AtomicBoolean(false);

        // tries to evict this chunk if noone is holding onto its resources anymore
        public boolean tryEvict() {
            if (refCount() <= 1 && evicted.compareAndSet(false, true)) {
                logger.trace("evicted {} with channel offset {}", regionKey, physicalStartOffset());
                decRef();
                return true;
            }
            return false;
        }

        public boolean forceEvict() {
            if (evicted.compareAndSet(false, true)) {
                logger.trace("force evicted {} with channel offset {}", regionKey, physicalStartOffset());
                decRef();
                return true;
            }
            return false;
        }

        public boolean isEvicted() {
            return evicted.get();
        }

        public boolean isReleased() {
            return isEvicted() && refCount() == 0;
        }

        @Override
        protected void closeInternal() {
            // now actually free the region associated with this chunk
            onClose(this);
            logger.trace("closed {} with channel offset {}", regionKey, physicalStartOffset());
        }

        private void ensureOpen() {
            if (evicted.get()) {
                throwAlreadyEvicted();
            }
        }

        private void throwAlreadyEvicted() {
            throw new AlreadyClosedException("File chunk is evicted");
        }

        public StepListener<Integer> populateAndRead(
            final ByteRange rangeToWrite,
            final ByteRange rangeToRead,
            final RangeAvailableHandler reader,
            final RangeMissingHandler writer,
            final Executor executor
        ) {
            final StepListener<Integer> listener = new StepListener<>();
            Releasable decrementRef = null;
            try {
                ensureOpen();
                incRef();
                decrementRef = Releasables.releaseOnce(this::decRef);
                ensureOpen();
                Releasable finalDecrementRef = decrementRef;
                listener.whenComplete(integer -> finalDecrementRef.close(), throwable -> finalDecrementRef.close());
                final SharedBytes.IO fileChannel = sharedBytes.getFileChannel(sharedBytesPos, tracker.getLength() <= smallRegionSize);
                listener.whenComplete(integer -> fileChannel.decRef(), e -> fileChannel.decRef());
                final ActionListener<Void> rangeListener = rangeListener(rangeToRead, reader, listener, fileChannel);
                if (rangeToRead.length() == 0L) {
                    // nothing to read, skip
                    rangeListener.onResponse(null);
                    return listener;
                }
                final List<SparseFileTracker.Gap> gaps = tracker.waitForRange(rangeToWrite, rangeToRead, rangeListener);

                for (SparseFileTracker.Gap gap : gaps) {
                    executor.execute(new AbstractRunnable() {

                        @Override
                        protected void doRun() throws Exception {
                            if (CacheFileRegion.this.tryIncRef() == false) {
                                // assert false : "expected a non-closed channel reference";
                                throw new AlreadyClosedException("Cache file channel has been released and closed");
                            }
                            try {
                                ensureOpen();
                                final long start = gap.start();
                                assert regionOwners[sharedBytesPos].get() == CacheFileRegion.this;
                                writer.fillCacheRange(
                                    fileChannel,
                                    physicalStartOffset() + gap.start(),
                                    gap.start(),
                                    gap.end() - gap.start(),
                                    progress -> gap.onProgress(start + progress)
                                );
                            } finally {
                                decRef();
                            }
                            gap.onCompletion();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            gap.onFailure(e);
                        }
                    });
                }
            } catch (Exception e) {
                releaseAndFail(listener, decrementRef, e);
            }
            return listener;
        }

        @Nullable
        public StepListener<Integer> readIfAvailableOrPending(final ByteRange rangeToRead, final RangeAvailableHandler reader) {
            final StepListener<Integer> listener = new StepListener<>();
            Releasable decrementRef = null;
            try {
                ensureOpen();
                incRef();
                decrementRef = Releasables.releaseOnce(this::decRef);
                ensureOpen();
                final Releasable finalDecrementRef = decrementRef;
                listener.whenComplete(integer -> finalDecrementRef.close(), throwable -> finalDecrementRef.close());
                final SharedBytes.IO fileChannel = sharedBytes.getFileChannel(sharedBytesPos, tracker.getLength() <= smallRegionSize);
                listener.whenComplete(integer -> fileChannel.decRef(), e -> fileChannel.decRef());
                if (tracker.waitForRangeIfPending(rangeToRead, rangeListener(rangeToRead, reader, listener, fileChannel))) {
                    return listener;
                } else {
                    IOUtils.close(decrementRef, fileChannel::decRef);
                    return null;
                }
            } catch (Exception e) {
                releaseAndFail(listener, decrementRef, e);
                return listener;
            }
        }

        private ActionListener<Void> rangeListener(
            ByteRange rangeToRead,
            RangeAvailableHandler reader,
            ActionListener<Integer> listener,
            SharedBytes.IO fileChannel
        ) {
            return ActionListener.wrap(success -> {
                final long physicalStartOffset = physicalStartOffset();
                assert regionOwners[sharedBytesPos].get() == CacheFileRegion.this;
                final int read = reader.onRangeAvailable(
                    fileChannel,
                    physicalStartOffset + rangeToRead.start(),
                    rangeToRead.start(),
                    rangeToRead.length()
                );
                assert read == rangeToRead.length() : "partial read ["
                    + read
                    + "] does not match the range to read ["
                    + rangeToRead.end()
                    + '-'
                    + rangeToRead.start()
                    + ']';
                listener.onResponse(read);
            }, listener::onFailure);
        }

        private void releaseAndFail(ActionListener<Integer> listener, Releasable decrementRef, Exception e) {
            try {
                Releasables.close(decrementRef);
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }
            listener.onFailure(e);
        }

        @Override
        protected void alreadyClosed() {
            throwAlreadyEvicted();
        }
    }

    public class FrozenCacheFile {

        private final CacheKey cacheKey;
        private final long fileSize;

        public FrozenCacheFile(CacheKey cacheKey, long fileSize) {
            this.cacheKey = cacheKey;
            this.fileSize = fileSize;
        }

        public StepListener<Integer> populateAndRead(
            final ByteRange rangeToWrite,
            final ByteRange rangeToRead,
            final RangeAvailableHandler reader,
            final RangeMissingHandler writer,
            final Executor executor
        ) {
            StepListener<Integer> stepListener = null;
            for (int i = getRegion(rangeToWrite.start(), fileSize); i <= getRegion(rangeToWrite.end(), fileSize); i++) {
                final int region = i;
                final ByteRange subRangeToWrite = mapSubRangeToRegion(rangeToWrite, region, fileSize);
                final ByteRange subRangeToRead = mapSubRangeToRegion(rangeToRead, region, fileSize);
                final CacheFileRegion fileRegion = get(cacheKey, fileSize, region);
                final StepListener<Integer> lis = fileRegion.populateAndRead(
                    subRangeToWrite,
                    subRangeToRead,
                    (channel, channelPos, relativePos, length) -> {
                        long distanceToStart = distToStart(rangeToRead.start(), region, fileRegion, channelPos, relativePos, length);
                        assert regionOwners[fileRegion.sharedBytesPos].get() == fileRegion;
                        return reader.onRangeAvailable(channel, channelPos, distanceToStart, length);
                    },
                    (channel, channelPos, relativePos, length, progressUpdater) -> {
                        long distanceToStart = distToStart(rangeToWrite.start(), region, fileRegion, channelPos, relativePos, length);
                        assert regionOwners[fileRegion.sharedBytesPos].get() == fileRegion;
                        writer.fillCacheRange(channel, channelPos, distanceToStart, length, progressUpdater);
                    },
                    executor
                );
                assert lis != null;
                if (stepListener == null) {
                    stepListener = lis;
                } else {
                    stepListener = stepListener.thenCombine(lis, Math::addExact);
                }

            }
            return stepListener;
        }

        private long distToStart(long start, int region, CacheFileRegion fileRegion, long channelPos, long relativePos, long length) {
            final long distanceToStart = region == getRegion(start, fileRegion.fileSize)
                ? relativePos - getRegionRelativePosition(start, fileRegion.fileSize)
                : getRegionStart(region, fileRegion.fileSize) + relativePos - start;
            assert channelPos >= fileRegion.physicalStartOffset() && channelPos + length <= fileRegion.physicalEndOffset();
            return distanceToStart;
        }

        @Nullable
        public StepListener<Integer> readIfAvailableOrPending(final ByteRange rangeToRead, final RangeAvailableHandler reader) {
            StepListener<Integer> stepListener = null;
            final long start = rangeToRead.start();
            for (int i = getRegion(rangeToRead.start(), fileSize); i <= getRegion(rangeToRead.end(), fileSize); i++) {
                final int region = i;
                final CacheFileRegion fileRegion = get(cacheKey, fileSize, region);
                final ByteRange subRangeToRead = mapSubRangeToRegion(rangeToRead, region, fileRegion.fileSize);
                final StepListener<Integer> lis = fileRegion.readIfAvailableOrPending(
                    subRangeToRead,
                    (channel, channelPos, relativePos, length) -> reader.onRangeAvailable(
                        channel,
                        channelPos,
                        distToStart(start, region, fileRegion, channelPos, relativePos, length),
                        length
                    )
                );
                if (lis == null) {
                    return null;
                }
                if (stepListener == null) {
                    stepListener = lis;
                } else {
                    stepListener = stepListener.thenCombine(lis, Math::addExact);
                }
            }
            return stepListener;
        }

        @Override
        public String toString() {
            return "SharedCacheFile{" + "cacheKey=" + cacheKey + ", length=" + fileSize + '}';
        }
    }

    public FrozenCacheFile getFrozenCacheFile(CacheKey cacheKey, long length) {
        return new FrozenCacheFile(cacheKey, length);
    }

    @FunctionalInterface
    public interface RangeAvailableHandler {
        // caller that wants to read from x should instead do a positional read from x + relativePos
        // caller should also only read up to length, further bytes will be offered by another call to this method
        int onRangeAvailable(SharedBytes.IO channel, long channelPos, long relativePos, long length) throws IOException;
    }

    @FunctionalInterface
    public interface RangeMissingHandler {
        void fillCacheRange(SharedBytes.IO channel, long channelPos, long relativePos, long length, Consumer<Long> progressUpdater)
            throws IOException;
    }
}
