/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.OffloadedLedgerHandle;
import org.apache.bookkeeper.mledger.offload.jcloud.BackedInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexEntry;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.DataBlockUtils.VersionCheck;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.KeyNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.GetOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobStoreBackedReadHandleImpl implements ReadHandle, OffloadedLedgerHandle {
    private static final Logger log = LoggerFactory.getLogger(BlobStoreBackedReadHandleImpl.class);

    // Entry header: 4 bytes length + 8 bytes entryId
    private static final int ENTRY_HEADER_SIZE = 4 + 8;

    protected static final AtomicIntegerFieldUpdater<BlobStoreBackedReadHandleImpl> PENDING_READ_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(BlobStoreBackedReadHandleImpl.class, "pendingRead");

    private final long ledgerId;
    private final OffloadIndexBlock index;
    private final BackedInputStream inputStream;
    private final DataInputStream dataStream;
    private final ExecutorService executor;
    private final OffsetsCache entryOffsetsCache;
    private final ChunkCache chunkCache;
    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();

    // New fields for chunk-cache and direct-fetch read paths
    private final BlobStore blobStore;
    private final String bucket;
    private final String dataObjectKey;
    private final long dataObjectLen;
    private final int chunkSize;
    private final boolean hasPerEntryIndex;

    enum State {
        Opened,
        Closed
    }

    private volatile State state = null;

    private volatile int pendingRead;

    private volatile long lastAccessTimestamp = System.currentTimeMillis();

    @VisibleForTesting
    BlobStoreBackedReadHandleImpl(long ledgerId, OffloadIndexBlock index,
                                          BackedInputStream inputStream, ExecutorService executor,
                                          OffsetsCache entryOffsetsCache, ChunkCache chunkCache) {
        this.ledgerId = ledgerId;
        this.index = index;
        this.inputStream = inputStream;
        this.dataStream = new DataInputStream(inputStream);
        this.executor = executor;
        this.entryOffsetsCache = entryOffsetsCache;
        this.chunkCache = chunkCache;
        // Legacy constructor — no blob store references for new read path
        this.blobStore = null;
        this.bucket = null;
        this.dataObjectKey = null;
        this.dataObjectLen = 0;
        this.chunkSize = 0;
        this.hasPerEntryIndex = false;
        state = State.Opened;
    }

    BlobStoreBackedReadHandleImpl(long ledgerId, OffloadIndexBlock index,
                                  BackedInputStream inputStream, ExecutorService executor,
                                  OffsetsCache entryOffsetsCache, ChunkCache chunkCache,
                                  BlobStore blobStore, String bucket, String dataObjectKey,
                                  long dataObjectLen, boolean hasPerEntryIndex) {
        this.ledgerId = ledgerId;
        this.index = index;
        this.inputStream = inputStream;
        this.dataStream = new DataInputStream(inputStream);
        this.executor = executor;
        this.entryOffsetsCache = entryOffsetsCache;
        this.chunkCache = chunkCache;
        this.blobStore = blobStore;
        this.bucket = bucket;
        this.dataObjectKey = dataObjectKey;
        this.dataObjectLen = dataObjectLen;
        this.chunkSize = chunkCache != null ? chunkCache.getChunkSize() : 0;
        this.hasPerEntryIndex = hasPerEntryIndex;
        state = State.Opened;
    }

    @Override
    public long getId() {
        return ledgerId;
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return index.getLedgerMetadata();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (closeFuture.get() != null || !closeFuture.compareAndSet(null, new CompletableFuture<>())) {
            return closeFuture.get();
        }

        CompletableFuture<Void> promise = closeFuture.get();
        executor.execute(() -> {
            try {
                index.close();
                inputStream.close();
                state = State.Closed;
                promise.complete(null);
            } catch (IOException t) {
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    // ---- New helper methods for ChunkCache + Direct Fetch read paths ----

    /**
     * Load a chunk from the blob store. Used as the loader callback for ChunkCache.get().
     */
    private ByteBuf loadChunk(long chunkStartOffset) throws IOException {
        long endRange = Math.min(chunkStartOffset + chunkSize - 1, dataObjectLen - 1);
        int bytesToRead = (int) (endRange - chunkStartOffset + 1);
        Blob blob = blobStore.getBlob(bucket, dataObjectKey,
                new GetOptions().range(chunkStartOffset, endRange));
        if (blob == null) {
            throw new IOException("Blob not found: " + dataObjectKey + " in " + bucket);
        }
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(bytesToRead, bytesToRead);
        try (InputStream stream = blob.getPayload().openStream()) {
            buf.writeBytes(stream, bytesToRead);
        } catch (Exception e) {
            buf.release();
            throw e instanceof IOException ? (IOException) e : new IOException(e);
        }
        return buf;
    }

    /**
     * Read bytes from the chunk cache, handling cross-chunk boundaries.
     * The returned ByteBuf is newly allocated and must be released by the caller.
     */
    private ByteBuf readBytesFromChunkCache(long startOffset, int totalLength)
            throws IOException, ExecutionException {
        ByteBuf result = PulsarByteBufAllocator.DEFAULT.buffer(totalLength, totalLength);
        int remaining = totalLength;
        long currentOffset = startOffset;

        try {
            while (remaining > 0) {
                long chunkStart = (currentOffset / chunkSize) * chunkSize;
                ChunkCache.ChunkKey chunkKey = new ChunkCache.ChunkKey(dataObjectKey, chunkStart);
                ByteBuf chunk = chunkCache.get(chunkKey, () -> loadChunk(chunkStart));
                chunk.retain();
                try {
                    int posInChunk = (int) (currentOffset - chunkStart);
                    int bytesAvailable = chunk.readableBytes() - posInChunk;
                    int toRead = Math.min(remaining, bytesAvailable);
                    result.writeBytes(chunk, posInChunk, toRead);
                    remaining -= toRead;
                    currentOffset += toRead;
                } finally {
                    chunk.release();
                }
            }
        } catch (Exception e) {
            result.release();
            if (e instanceof ExecutionException) {
                throw (ExecutionException) e;
            }
            throw e instanceof IOException ? (IOException) e : new IOException(e);
        }

        return result;
    }

    /**
     * Read bytes via a single HTTP range request (direct fetch, bypassing chunk cache).
     * Used for large reads that exceed the chunk size.
     * The returned ByteBuf is newly allocated and must be released by the caller.
     */
    private ByteBuf readBytesDirectFetch(long startOffset, long endOffset) throws IOException {
        int bytesToRead = (int) (endOffset - startOffset);
        // endOffset is exclusive, range is inclusive
        Blob blob = blobStore.getBlob(bucket, dataObjectKey,
                new GetOptions().range(startOffset, endOffset - 1));
        if (blob == null) {
            throw new IOException("Blob not found: " + dataObjectKey + " in " + bucket);
        }
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(bytesToRead, bytesToRead);
        try (InputStream stream = blob.getPayload().openStream()) {
            buf.writeBytes(stream, bytesToRead);
        } catch (Exception e) {
            buf.release();
            throw e instanceof IOException ? (IOException) e : new IOException(e);
        }
        return buf;
    }

    /**
     * Scan forward from a known offset to find the offset of a target entry.
     * This is the old-format fallback for when OffsetsCache misses.
     * Reads through chunk cache and caches all intermediate offsets found during the scan.
     * Reads directly from cached chunks to avoid per-header ByteBuf allocations.
     *
     * @return the byte offset of the target entry, or -1 if not found
     */
    private long scanForEntryOffset(long targetEntryId) throws IOException, ExecutionException {
        OffloadIndexEntry indexEntry = index.getIndexEntryForEntry(targetEntryId);
        if (indexEntry.getEntryId() == targetEntryId) {
            long offset = indexEntry.getDataOffset();
            entryOffsetsCache.put(ledgerId, targetEntryId, offset);
            return offset;
        }

        // Start scanning from the block-level index entry
        long scanOffset = indexEntry.getDataOffset();
        long scanEntryId = indexEntry.getEntryId();
        long targetOffset = -1;

        // Collect all discovered offsets and bulk-put at the end
        Map<Long, Long> discoveredOffsets = new HashMap<>();

        // Keep a reference to the current chunk to avoid repeated cache lookups
        ByteBuf currentChunk = null;
        long currentChunkStart = -1;

        try {
            while (scanEntryId <= targetEntryId) {
                long chunkStart = (scanOffset / chunkSize) * chunkSize;

                // Load chunk if we don't have it or moved to a different chunk
                if (currentChunk == null || chunkStart != currentChunkStart) {
                    if (currentChunk != null) {
                        currentChunk.release();
                    }
                    ChunkCache.ChunkKey chunkKey = new ChunkCache.ChunkKey(dataObjectKey, chunkStart);
                    currentChunk = chunkCache.get(chunkKey, () -> loadChunk(chunkStart));
                    currentChunk.retain();
                    currentChunkStart = chunkStart;
                }

                int posInChunk = (int) (scanOffset - currentChunkStart);
                int bytesAvailable = currentChunk.readableBytes() - posInChunk;

                int length;
                long entryId;

                // Need at least ENTRY_HEADER_SIZE (12) bytes for both length + entryId
                if (bytesAvailable < ENTRY_HEADER_SIZE) {
                    // Entry header spans chunk boundary — use readBytesFromChunkCache for this rare case
                    ByteBuf headerBuf = readBytesFromChunkCache(scanOffset, ENTRY_HEADER_SIZE);
                    try {
                        length = headerBuf.readInt();
                        if (length < 0) {
                            OffloadIndexEntry nextBlockEntry = index.getIndexEntryForEntry(scanEntryId + 1);
                            if (nextBlockEntry.getEntryId() <= scanEntryId) {
                                break;
                            }
                            scanOffset = nextBlockEntry.getDataOffset();
                            scanEntryId = nextBlockEntry.getEntryId();
                            continue;
                        }
                        entryId = headerBuf.readLong();
                    } finally {
                        headerBuf.release();
                    }
                } else {
                    // Read directly from the chunk buffer (no allocation)
                    length = currentChunk.getInt(posInChunk);
                    if (length < 0) {
                        OffloadIndexEntry nextBlockEntry = index.getIndexEntryForEntry(scanEntryId + 1);
                        if (nextBlockEntry.getEntryId() <= scanEntryId) {
                            break;
                        }
                        scanOffset = nextBlockEntry.getDataOffset();
                        scanEntryId = nextBlockEntry.getEntryId();
                        continue;
                    }
                    entryId = currentChunk.getLong(posInChunk + 4);
                }

                // Collect the offset (will bulk-put at the end)
                discoveredOffsets.put(entryId, scanOffset);

                if (entryId == targetEntryId) {
                    targetOffset = scanOffset;
                    break;
                }

                // Advance past this entry: header (12 bytes) + payload
                scanOffset += ENTRY_HEADER_SIZE + length;
                scanEntryId = entryId + 1;
            }
        } finally {
            if (currentChunk != null) {
                currentChunk.release();
            }
        }

        // Bulk-put all discovered offsets into cache at once
        if (!discoveredOffsets.isEmpty()) {
            entryOffsetsCache.bulkPut(ledgerId, discoveredOffsets);
        }

        return targetOffset;
    }

    /**
     * Parse entries from a ByteBuf containing raw data read from the data object.
     * The data starts at dataStartOffset in the data object coordinate space.
     * We extract entries whose IDs fall in [firstEntry, lastEntry].
     */
    private List<LedgerEntry> parseEntries(ByteBuf data, long dataStartOffset,
                                           long firstEntry, long lastEntry) {
        List<LedgerEntry> entries = new ArrayList<>();
        long currentOffset = dataStartOffset;

        while (data.readableBytes() >= ENTRY_HEADER_SIZE && entries.size() < (lastEntry - firstEntry + 1)) {
            int length = data.readInt();
            if (length < 0) {
                // Padding — skip remaining in this block. This shouldn't normally happen
                // in the new read path since we use exact offsets, but handle defensively.
                break;
            }
            long entryId = data.readLong();

            if (data.readableBytes() < length) {
                // Not enough data for this entry's payload — truncated read
                log.warn("Truncated entry data for ledger {} entry {} at offset {}, expected {} bytes but only {} "
                        + "available", ledgerId, entryId, currentOffset, length, data.readableBytes());
                break;
            }

            // Cache the offset for this entry
            entryOffsetsCache.put(ledgerId, entryId, currentOffset);

            if (entryId >= firstEntry && entryId <= lastEntry) {
                ByteBuf entryBuf = PulsarByteBufAllocator.DEFAULT.buffer(length, length);
                data.readBytes(entryBuf, length);
                entries.add(LedgerEntryImpl.create(ledgerId, entryId, length, entryBuf));
            } else {
                // Skip this entry's payload
                data.skipBytes(length);
            }

            currentOffset += ENTRY_HEADER_SIZE + length;
        }

        return entries;
    }

    /**
     * Check whether the new chunk-cache/direct-fetch read path can be used.
     * Returns true if blob store references and chunk cache are available.
     */
    private boolean canUseNewReadPath() {
        return blobStore != null && chunkCache != null && chunkSize > 0;
    }

    private class ReadTask implements Runnable {
        private final long firstEntry;
        private final long lastEntry;
        private final CompletableFuture<LedgerEntries> promise;
        private int seekedAndTryTimes = 0;

        public ReadTask(long firstEntry, long lastEntry, CompletableFuture<LedgerEntries> promise) {
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.promise = promise;
        }

        @Override
        public void run() {
            if (state == State.Closed) {
                log.warn("Reading a closed read handler. Ledger ID: {}, Read range: {}-{}",
                        ledgerId, firstEntry, lastEntry);
                promise.completeExceptionally(new ManagedLedgerException.OffloadReadHandleClosedException());
                return;
            }

            try {
                if (firstEntry > lastEntry
                        || firstEntry < 0
                        || lastEntry > getLastAddConfirmed()) {
                    promise.completeExceptionally(new BKException.BKIncorrectParameterException());
                    return;
                }

                if (canUseNewReadPath()) {
                    runNewPath();
                } else {
                    runLegacyPath();
                }
            } catch (Throwable t) {
                handleReadFailure(t);
            }
        }

        /**
         * New read path using ChunkCache and direct fetch.
         */
        private void runNewPath() throws Exception {
            List<LedgerEntry> entryCollector = new ArrayList<>();
            try {
                // Step 1: Look up all entry offsets from OffsetsCache
                long[] offsets = new long[(int) (lastEntry - firstEntry + 1)];
                boolean allOffsetsKnown = true;

                for (long entryId = firstEntry; entryId <= lastEntry; entryId++) {
                    int idx = (int) (entryId - firstEntry);
                    Long cachedOffset = entryOffsetsCache.getIfPresent(ledgerId, entryId);
                    if (cachedOffset != null) {
                        offsets[idx] = cachedOffset;
                    } else {
                        allOffsetsKnown = false;
                        offsets[idx] = -1;
                        // As soon as we find a miss, stop probing the rest — we know
                        // we'll need to resolve missing offsets below.
                        break;
                    }
                }

                // Step 1b: Resolve missing offsets
                if (!allOffsetsKnown) {
                    if (!hasPerEntryIndex) {
                        // Old format — no per-entry index. Use legacy path which handles
                        // sequential reading efficiently without per-entry offset lookups.
                        runLegacyPath();
                        return;
                    }
                    // Per-entry index exists but some entries weren't pre-loaded.
                    // Fill remaining from cache/scan.
                    int firstMissingIdx = 0;
                    for (int i = 0; i < offsets.length; i++) {
                        if (offsets[i] == -1) {
                            firstMissingIdx = i;
                            break;
                        }
                    }
                    for (int i = firstMissingIdx; i < offsets.length; i++) {
                        if (offsets[i] == -1) {
                            long entryId = firstEntry + i;
                            Long cached = entryOffsetsCache.getIfPresent(ledgerId, entryId);
                            if (cached != null) {
                                offsets[i] = cached;
                            } else {
                                long scanned = scanForEntryOffset(entryId);
                                if (scanned < 0) {
                                    throw new BKException.BKUnexpectedConditionException();
                                }
                                offsets[i] = scanned;
                                // Scan may have populated subsequent entries
                                for (int j = i + 1; j < offsets.length; j++) {
                                    if (offsets[j] == -1) {
                                        Long nowCached = entryOffsetsCache.getIfPresent(
                                                ledgerId, firstEntry + j);
                                        if (nowCached != null) {
                                            offsets[j] = nowCached;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Step 2: Compute total byte range
                // We know the start offset of the first entry. For the end, we need to know
                // the length of the last entry. We read the last entry's header to find its length.
                long startOffset = offsets[0];

                // To find the end offset, we need the length of the last entry.
                // Read the last entry's header (4 bytes) to get its length.
                long lastEntryOffset = offsets[offsets.length - 1];
                ByteBuf lastEntryHeader = readBytesFromChunkCache(lastEntryOffset, 4);
                int lastEntryLength;
                try {
                    lastEntryLength = lastEntryHeader.readInt();
                } finally {
                    lastEntryHeader.release();
                }

                long endOffset = lastEntryOffset + ENTRY_HEADER_SIZE + lastEntryLength;
                long totalBytes = endOffset - startOffset;

                // Step 3: Choose fetch strategy and read data
                ByteBuf rawData;
                if (totalBytes <= chunkSize) {
                    // Chunk cache path: read through chunk cache (read-ahead benefit)
                    rawData = readBytesFromChunkCache(startOffset, (int) totalBytes);
                } else {
                    // Direct fetch path: single HTTP range request
                    rawData = readBytesDirectFetch(startOffset, endOffset);
                }

                // Step 4: Parse entries from the fetched data
                try {
                    entryCollector = parseEntries(rawData, startOffset, firstEntry, lastEntry);
                } finally {
                    rawData.release();
                }

                if (entryCollector.size() != (lastEntry - firstEntry + 1)) {
                    log.error("Expected {} entries but parsed {} for ledger {} range [{}, {}]",
                            (lastEntry - firstEntry + 1), entryCollector.size(),
                            ledgerId, firstEntry, lastEntry);
                    entryCollector.forEach(LedgerEntry::close);
                    throw new BKException.BKUnexpectedConditionException();
                }

                promise.complete(LedgerEntriesImpl.create(entryCollector));
            } catch (Throwable t) {
                entryCollector.forEach(LedgerEntry::close);
                throw t;
            }
        }

        /**
         * Legacy read path using BackedInputStream (for backward compatibility when
         * blob store references are not available, e.g., in tests using the old constructor).
         */
        private void runLegacyPath() throws Exception {
            List<LedgerEntry> entryCollector = new ArrayList<>();
            try {
                long entriesToRead = (lastEntry - firstEntry) + 1;
                long expectedEntryId = firstEntry;
                seekToEntryOffset(expectedEntryId);
                seekedAndTryTimes++;

                while (entriesToRead > 0) {
                    long currentPosition = inputStream.getCurrentPosition();
                    int length = dataStream.readInt();
                    if (length < 0) { // hit padding or new block
                        seekToEntryOffset(expectedEntryId);
                        continue;
                    }
                    long entryId = dataStream.readLong();
                    if (entryId == expectedEntryId) {
                        entryOffsetsCache.put(ledgerId, entryId, currentPosition);
                        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(length, length);
                        entryCollector.add(LedgerEntryImpl.create(ledgerId, entryId, length, buf));
                        int toWrite = length;
                        while (toWrite > 0) {
                            toWrite -= buf.writeBytes(dataStream, toWrite);
                        }
                        entriesToRead--;
                        expectedEntryId++;
                    } else {
                        handleUnexpectedEntryId(expectedEntryId, entryId);
                    }
                }
                promise.complete(LedgerEntriesImpl.create(entryCollector));
            } catch (Throwable t) {
                entryCollector.forEach(LedgerEntry::close);
                throw t;
            }
        }

        private void handleReadFailure(Throwable t) {
            log.error("Failed to read entries {} - {} from the offloader in ledger {}",
                    firstEntry, lastEntry, ledgerId, t);
            if (t instanceof KeyNotFoundException) {
                promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsException());
            } else {
                promise.completeExceptionally(t);
            }
        }

        // --- Legacy path helper methods (kept for backward compatibility) ---

        private void handleUnexpectedEntryId(long expectedId, long actEntryId) throws Exception {
            LedgerMetadata ledgerMetadata = getLedgerMetadata();
            OffloadIndexEntry offsetOfExpectedId = index.getIndexEntryForEntry(expectedId);
            OffloadIndexEntry offsetOfActId = actEntryId <= getLedgerMetadata().getLastEntryId() && actEntryId >= 0
                    ? index.getIndexEntryForEntry(actEntryId) : null;
            String logLine = String.format("Failed to read [ %s ~ %s ] of the ledger %s."
                    + " Because got a incorrect entry id %s, the offset is %s."
                    + " The expected entry id is %s, the offset is %s."
                    + " Have seeked and retry read times: %s. LAC is %s.",
                    firstEntry, lastEntry, ledgerId,
                    actEntryId, offsetOfActId == null ? "null because it does not exist"
                            : String.valueOf(offsetOfActId),
                    expectedId, String.valueOf(offsetOfExpectedId),
                    seekedAndTryTimes, ledgerMetadata != null ? ledgerMetadata.getLastEntryId() : "unknown");
            long maxTryTimes = Math.max(3, (lastEntry - firstEntry + 1) >> 2);
            if (seekedAndTryTimes > maxTryTimes) {
                log.error(logLine);
                throw new BKException.BKUnexpectedConditionException();
            } else {
                log.warn(logLine);
            }
            seekToEntryOffset(expectedId);
            seekedAndTryTimes++;
        }

        private void skipPreviousEntry(long startEntryId, long expectedEntryId) throws IOException, BKException {
            long nextExpectedEntryId = startEntryId;
            while (nextExpectedEntryId < expectedEntryId) {
                long offset = inputStream.getCurrentPosition();
                int len = dataStream.readInt();
                if (len < 0) {
                    LedgerMetadata ledgerMetadata = getLedgerMetadata();
                    OffloadIndexEntry offsetOfExpectedId = index.getIndexEntryForEntry(expectedEntryId);
                    log.error("Failed to read [ {} ~ {} ] of the ledger {}."
                        + " Because failed to skip a previous entry {}, len: {}, got a negative len."
                        + " The expected entry id is {}, the offset is {}."
                        + " Have seeked and retry read times: {}. LAC is {}.",
                        firstEntry, lastEntry, ledgerId,
                        nextExpectedEntryId, len,
                        expectedEntryId, String.valueOf(offsetOfExpectedId),
                        seekedAndTryTimes, ledgerMetadata != null ? ledgerMetadata.getLastEntryId() : "unknown");
                    throw new BKException.BKUnexpectedConditionException();
                }
                long entryId = dataStream.readLong();
                if (entryId == nextExpectedEntryId) {
                    long skipped = inputStream.skip(len);
                    if (skipped != len) {
                        LedgerMetadata ledgerMetadata = getLedgerMetadata();
                        OffloadIndexEntry offsetOfExpectedId = index.getIndexEntryForEntry(expectedEntryId);
                        log.error("Failed to read [ {} ~ {} ] of the ledger {}."
                            + " Because failed to skip a previous entry {}, offset: {}, len: {}, "
                            + "there is no more data."
                            + " The expected entry id is {}, the offset is {}."
                            + " Have seeked and retry read times: {}. LAC is {}.",
                            firstEntry, lastEntry, ledgerId,
                            entryId, offset, len,
                            expectedEntryId, String.valueOf(offsetOfExpectedId),
                            seekedAndTryTimes, ledgerMetadata != null ? ledgerMetadata.getLastEntryId() : "unknown");
                        throw new BKException.BKUnexpectedConditionException();
                    }
                    nextExpectedEntryId++;
                } else {
                    LedgerMetadata ledgerMetadata = getLedgerMetadata();
                    OffloadIndexEntry offsetOfExpectedId = index.getIndexEntryForEntry(expectedEntryId);
                    log.error("Failed to read [ {} ~ {} ] of the ledger {}."
                        + " Because got a incorrect entry id {},."
                        + " The expected entry id is {}, the offset is {}."
                        + " Have seeked and retry read times: {}. LAC is {}.",
                        firstEntry, lastEntry, ledgerId,
                        entryId, expectedEntryId, String.valueOf(offsetOfExpectedId),
                        seekedAndTryTimes, ledgerMetadata != null ? ledgerMetadata.getLastEntryId() : "unknown");
                    throw new BKException.BKUnexpectedConditionException();
                }
            }
        }

        private void seekToEntryOffset(long expectedEntryId) throws IOException, BKException {
            // 1. Try to find the precise index.
            // 1-1. Precise cached indexes.
            Long cachedPreciseIndex = entryOffsetsCache.getIfPresent(ledgerId, expectedEntryId);
            if (cachedPreciseIndex != null) {
                inputStream.seek(cachedPreciseIndex);
                return;
            }
            // 1-2. Precise persistent indexes.
            OffloadIndexEntry indexOfNearestEntry = index.getIndexEntryForEntry(expectedEntryId);
            if (indexOfNearestEntry.getEntryId() == expectedEntryId) {
                inputStream.seek(indexOfNearestEntry.getDataOffset());
                return;
            }
            // 2. Try to use the previous index.
            Long cachedPreviousKnownOffset = entryOffsetsCache.getIfPresent(ledgerId, expectedEntryId - 1);
            if (cachedPreviousKnownOffset != null) {
                inputStream.seek(cachedPreviousKnownOffset);
                skipPreviousEntry(expectedEntryId - 1, expectedEntryId);
                return;
            }
            // 3. Use the persistent index of the nearest entry that is smaller than "expectedEntryId".
            if (indexOfNearestEntry.getEntryId() < expectedEntryId) {
                inputStream.seek(indexOfNearestEntry.getDataOffset());
                skipPreviousEntry(indexOfNearestEntry.getEntryId(), expectedEntryId);
            } else {
                LedgerMetadata ledgerMetadata = getLedgerMetadata();
                log.error("Failed to read [ {} ~ {} ] of the ledger {}."
                    + " Because got a incorrect index {} of the entry {}, which is greater than expected."
                    + " Have seeked and retry read times: {}. LAC is {}.",
                    firstEntry, lastEntry, ledgerId,
                    String.valueOf(indexOfNearestEntry), expectedEntryId,
                    seekedAndTryTimes, ledgerMetadata != null ? ledgerMetadata.getLastEntryId() : "unknown");
                throw new BKException.BKUnexpectedConditionException();
            }
        }
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        if (log.isDebugEnabled()) {
            log.debug("Ledger {}: reading {} - {} ({} entries}",
                    getId(), firstEntry, lastEntry, (1 + lastEntry - firstEntry));
        }
        CompletableFuture<LedgerEntries> promise = new CompletableFuture<>();

        // Ledger handles will be only marked idle when "pendingRead" is "0", it is not needed to update
        // "lastAccessTimestamp" if "pendingRead" is larger than "0".
        // Rather than update "lastAccessTimestamp" when starts a reading, updating it when a reading task is finished
        // is better.
        PENDING_READ_UPDATER.incrementAndGet(this);
        promise.whenComplete((__, ex) -> {
            lastAccessTimestamp = System.currentTimeMillis();
            PENDING_READ_UPDATER.decrementAndGet(BlobStoreBackedReadHandleImpl.this);
        });
        executor.execute(new ReadTask(firstEntry, lastEntry, promise));
        return promise;
    }

    private void seekToEntry(long nextExpectedId) throws IOException {
        Long knownOffset = entryOffsetsCache.getIfPresent(ledgerId, nextExpectedId);
        if (knownOffset != null) {
            inputStream.seek(knownOffset);
        } else {
            long dataOffset = index.getIndexEntryForEntry(nextExpectedId).getDataOffset();
            inputStream.seek(dataOffset);
        }
    }

    private void seekToEntry(OffloadIndexEntry offloadIndexEntry) throws IOException {
        long dataOffset = offloadIndexEntry.getDataOffset();
        inputStream.seek(dataOffset);
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        return readAsync(firstEntry, lastEntry);
    }

    @Override
    public CompletableFuture<Long> readLastAddConfirmedAsync() {
        return CompletableFuture.completedFuture(getLastAddConfirmed());
    }

    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
        return CompletableFuture.completedFuture(getLastAddConfirmed());
    }

    @Override
    public long getLastAddConfirmed() {
        return getLedgerMetadata().getLastEntryId();
    }

    @Override
    public long getLength() {
        return getLedgerMetadata().getLength();
    }

    @Override
    public boolean isClosed() {
        return getLedgerMetadata().isClosed();
    }

    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(long entryId,
                                                                                      long timeOutInMillis,
                                                                                      boolean parallel) {
        CompletableFuture<LastConfirmedAndEntry> promise = new CompletableFuture<>();
        promise.completeExceptionally(new UnsupportedOperationException());
        return promise;
    }

    public static ReadHandle open(ScheduledExecutorService executor,
                                  BlobStore blobStore, String bucket, String key, String indexKey,
                                  VersionCheck versionCheck,
                                  long ledgerId, int readBufferSize,
                                  LedgerOffloaderStats offloaderStats, String managedLedgerName,
                                  OffsetsCache entryOffsetsCache, ChunkCache chunkCache)
            throws IOException, BKException.BKNoSuchLedgerExistsException {
        int retryCount = 3;
        OffloadIndexBlock index = null;
        IOException lastException = null;
        String topicName = TopicName.fromPersistenceNamingEncoding(managedLedgerName);
        // The following retry is used to avoid to some network issue cause read index file failure.
        // If it can not recovery in the retry, we will throw the exception and the dispatcher will schedule to
        // next read.
        // If we use a backoff to control the retry, it will introduce a concurrent operation.
        // We don't want to make it complicated, because in the most of case it shouldn't in the retry loop.
        while (retryCount-- > 0) {
            long readIndexStartTime = System.nanoTime();
            Blob blob = blobStore.getBlob(bucket, indexKey);
            if (blob == null) {
                log.error("{} not found in container {}", indexKey, bucket);
                throw new BKException.BKNoSuchLedgerExistsException();
            }
            offloaderStats.recordReadOffloadIndexLatency(topicName,
                    System.nanoTime() - readIndexStartTime, TimeUnit.NANOSECONDS);
            versionCheck.check(indexKey, blob);
            OffloadIndexBlockBuilder indexBuilder = OffloadIndexBlockBuilder.create();
            try (InputStream payLoadStream = blob.getPayload().openStream()) {
                index = indexBuilder.fromStream(payLoadStream, ledgerId, entryOffsetsCache);
            } catch (IOException e) {
                // retry to avoid the network issue caused read failure
                log.warn("Failed to get index block from the offoaded index file {}, still have {} times to retry",
                    indexKey, retryCount, e);
                lastException = e;
                continue;
            }
            lastException = null;
            break;
        }
        if (lastException != null) {
            throw lastException;
        }

        BackedInputStream inputStream = new BlobStoreBackedInputStreamImpl(blobStore, bucket, key,
                versionCheck, index.getDataObjectLength(), readBufferSize, offloaderStats, managedLedgerName);

        // Detect if per-entry index was loaded by checking if entry 0 is in the cache
        boolean hasPerEntryIdx = entryOffsetsCache.getIfPresent(ledgerId,
                index.getIndexEntryForEntry(0).getEntryId()) != null;

        return new BlobStoreBackedReadHandleImpl(ledgerId, index, inputStream, executor, entryOffsetsCache,
                chunkCache, blobStore, bucket, key, index.getDataObjectLength(), hasPerEntryIdx);
    }

    // for testing
    @VisibleForTesting
    State getState() {
        return this.state;
    }

    @Override
    public long lastAccessTimestamp() {
        return lastAccessTimestamp;
    }

    @Override
    public int getPendingRead() {
        return PENDING_READ_UPDATER.get(this);
    }
}
