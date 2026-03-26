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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.mledger.LedgerOffloaderStatsDisable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Tiered storage read benchmark covering 3 dimensions:
 * <ul>
 *   <li>Access pattern: sequential vs random</li>
 *   <li>Cursor count: 1 vs 3 (simulates single vs multi-subscription)</li>
 *   <li>Ledger count: 1 vs 3 (simulates single vs multi-topic)</li>
 * </ul>
 *
 * <p>Each invocation uses a cold chunk cache to measure realistic first-read
 * performance. Blob store latency is simulated at 100ms per HTTP range request.
 *
 * <p>Each ledger has {@code numCursors} cursors, and each cursor reads all entries
 * in its ledger (10,000 entries across 4 blocks), ensuring reads cross multiple
 * block boundaries. Total reads per invocation = TOTAL_ENTRIES × numCursors ×
 * numLedgers. For multi-cursor sequential reads, cursors start at spread-out
 * positions within the ledger to simulate distinct subscriptions reading
 * different regions.
 *
 * <p>Data layout: 4 blocks × 2,500 entries × 1KB payload = ~10MB per ledger.
 * With 1MB chunks, each ledger occupies ~10 chunks in the cache. The 64MB chunk
 * cache holds ~64 chunks, so:
 * <ul>
 *   <li>1 ledger (10 chunks): fits comfortably, no eviction</li>
 *   <li>5 ledgers (50 chunks): fits but tight</li>
 *   <li>10 ledgers (100 chunks): exceeds capacity, eviction kicks in</li>
 * </ul>
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(1)
@State(Scope.Thread)
@Warmup(iterations = 0)
@Measurement(iterations = 1)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
@Fork(1)
public class TieredStorageReadBenchmark {

    private static final String BUCKET = "test-bucket";
    private static final int NUM_BLOCKS = 4;
    private static final int ENTRIES_PER_BLOCK = 2500;
    private static final int TOTAL_ENTRIES = NUM_BLOCKS * ENTRIES_PER_BLOCK;
    private static final int PAYLOAD_SIZE = 1024;
    private static final long LATENCY_MS = 100;
    private static final int READS_PER_CURSOR_RANDOM = 100;
    private static final String MANAGED_LEDGER_NAME = "public/default/persistent/bench-topic";

    @Param({"sequential", "random"})
    String accessPattern;

    @Param({"1", "3"})
    int numCursors;

    @Param({"1", "5", "10"})
    int numLedgers;

    // Per-ledger state
    private OffloadDataGenerator[] generators;
    private long[] ledgerIds;
    private String[] dataKeys;
    private String[] indexKeys;
    private BlobStoreBackedReadHandleImpl[] readHandles;

    // Shared state
    private MockBlobStore mockBlobStore;
    private ScheduledExecutorService executor;
    private OffsetsCache offsetsCache;
    private ChunkCache chunkCache;

    // Cursor positions for sequential reads: [ledgerIdx][cursorIdx]
    private int[][] cursorPositions;

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        int maxLedgers = 10;
        generators = new OffloadDataGenerator[maxLedgers];
        ledgerIds = new long[maxLedgers];
        dataKeys = new String[maxLedgers];
        indexKeys = new String[maxLedgers];

        mockBlobStore = new MockBlobStore(BUCKET, LATENCY_MS);
        executor = Executors.newScheduledThreadPool(numLedgers);
        offsetsCache = new OffsetsCache();
        chunkCache = new ChunkCache(64 * 1024 * 1024, 60, 1024 * 1024);

        for (int i = 0; i < maxLedgers; i++) {
            generators[i] = new OffloadDataGenerator(NUM_BLOCKS, ENTRIES_PER_BLOCK, PAYLOAD_SIZE);
            ledgerIds[i] = i + 1;
            dataKeys[i] = "data-" + (i + 1);
            indexKeys[i] = "index-" + (i + 1);

            byte[] data = generators[i].generateData();
            byte[] indexBytes = generators[i].generateSerializedIndex(ledgerIds[i]);
            mockBlobStore.addLedger(data, dataKeys[i], indexBytes, indexKeys[i]);
        }
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws Exception {
        // Close previous read handles
        if (readHandles != null) {
            for (BlobStoreBackedReadHandleImpl handle : readHandles) {
                if (handle != null) {
                    handle.closeAsync().join();
                }
            }
        }

        // Cold cache: reset both caches
        offsetsCache.clear();
        chunkCache.close();
        chunkCache = new ChunkCache(64 * 1024 * 1024, 60, 1024 * 1024);

        // Open read handles for the configured number of ledgers
        readHandles = new BlobStoreBackedReadHandleImpl[numLedgers];
        for (int i = 0; i < numLedgers; i++) {
            readHandles[i] = (BlobStoreBackedReadHandleImpl) BlobStoreBackedReadHandleImpl.open(
                    executor, mockBlobStore.getBlobStore(), BUCKET, dataKeys[i], indexKeys[i],
                    (k, b) -> {}, ledgerIds[i], 1024 * 1024,
                    LedgerOffloaderStatsDisable.INSTANCE, MANAGED_LEDGER_NAME, offsetsCache, chunkCache);
        }

        // Initialize cursor positions for sequential reads
        // Each ledger has numCursors cursors, spread evenly across the entry range
        cursorPositions = new int[numLedgers][numCursors];
        for (int l = 0; l < numLedgers; l++) {
            for (int c = 0; c < numCursors; c++) {
                cursorPositions[l][c] = (TOTAL_ENTRIES / numCursors) * c;
            }
        }

        mockBlobStore.resetRequestCount();
    }

    @Benchmark
    public void readEntries(Blackhole bh) throws Exception {
        // Each ledger has numCursors cursors
        // Sequential: each cursor reads all entries in the ledger
        // Random: each cursor reads READS_PER_CURSOR_RANDOM entries (subset)
        int readsPerCursor = "sequential".equals(accessPattern) ? TOTAL_ENTRIES : READS_PER_CURSOR_RANDOM;
        int totalReads = readsPerCursor * numCursors * numLedgers;

        for (int i = 0; i < totalReads; i++) {
            // Round-robin across ledgers, then cursors within each ledger
            int ledgerIdx = i % numLedgers;
            int cursorIdx = (i / numLedgers) % numCursors;
            BlobStoreBackedReadHandleImpl handle = readHandles[ledgerIdx];

            long entryId;
            if ("sequential".equals(accessPattern)) {
                entryId = cursorPositions[ledgerIdx][cursorIdx];
                cursorPositions[ledgerIdx][cursorIdx] =
                        (cursorPositions[ledgerIdx][cursorIdx] + 1) % TOTAL_ENTRIES;
            } else {
                entryId = ThreadLocalRandom.current().nextInt(TOTAL_ENTRIES);
            }

            LedgerEntries entries = handle.readAsync(entryId, entryId).get();
            bh.consume(entries);
            entries.close();
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (readHandles != null) {
            for (BlobStoreBackedReadHandleImpl handle : readHandles) {
                if (handle != null) {
                    handle.closeAsync().join();
                }
            }
        }
        executor.shutdownNow();
        offsetsCache.close();
        chunkCache.close();
        mockBlobStore.close();
    }
}
