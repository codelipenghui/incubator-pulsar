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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.testng.annotations.Test;

public class ChunkCacheTest {

    private static final int ONE_MB = 1024 * 1024;

    private ByteBuf createTestChunk(byte fillByte) {
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(ONE_MB, ONE_MB);
        for (int i = 0; i < ONE_MB; i++) {
            buf.writeByte(fillByte);
        }
        return buf;
    }

    @Test
    public void testGetAndCache() throws Exception {
        // Create ChunkCache with 10MB budget, 60s TTL, 1MB chunk size
        ChunkCache cache = new ChunkCache(10 * ONE_MB, 60, ONE_MB);
        try {
            ChunkCache.ChunkKey key = new ChunkCache.ChunkKey("test-object", 0);
            AtomicInteger loaderCallCount = new AtomicInteger(0);

            // First call — loader should be invoked
            ByteBuf result1 = cache.get(key, () -> {
                loaderCallCount.incrementAndGet();
                return createTestChunk((byte) 0xAB);
            });
            assertNotNull(result1);
            assertEquals(loaderCallCount.get(), 1);

            // Second call — loader should NOT be invoked (cached)
            ByteBuf result2 = cache.get(key, () -> {
                loaderCallCount.incrementAndGet();
                return createTestChunk((byte) 0xCD);
            });
            assertNotNull(result2);
            assertEquals(loaderCallCount.get(), 1);

            // Verify contents match the first load
            assertEquals(result1.getByte(0), (byte) 0xAB);
            assertEquals(result2.getByte(0), (byte) 0xAB);
        } finally {
            cache.close();
        }
    }

    @Test
    public void testEvictionOnCapacity() throws Exception {
        // Create ChunkCache with 2MB budget (holds ~2 chunks of 1MB each)
        ChunkCache cache = new ChunkCache(2 * ONE_MB, 60, ONE_MB);
        try {
            // Pre-create and retain buffers before inserting into the cache,
            // so that eviction during insert won't fully release them.
            List<ByteBuf> chunks = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                ByteBuf buf = createTestChunk((byte) (i + 1));
                buf.retain(); // extra ref so eviction doesn't fully release
                chunks.add(buf);
            }

            // Insert all 3 chunks into the cache (which now shares ownership)
            for (int i = 0; i < 3; i++) {
                ChunkCache.ChunkKey key = new ChunkCache.ChunkKey("test-object", (long) i * ONE_MB);
                final int idx = i;
                cache.get(key, () -> chunks.get(idx));
            }

            // Force cache maintenance (Guava caches do eviction on access)
            cache.get(new ChunkCache.ChunkKey("force-cleanup", 0), () -> createTestChunk((byte) 0xFF));

            // At least one of the first 3 chunks should have been evicted.
            // Evicted chunks: refCnt == 1 (only our retain remains).
            // Cached chunks: refCnt == 2 (cache + our retain).
            int evictedCount = 0;
            for (ByteBuf chunk : chunks) {
                if (chunk.refCnt() == 1) {
                    evictedCount++;
                }
            }
            assertTrue(evictedCount >= 1, "Expected at least 1 chunk to be evicted, but got " + evictedCount);

            // Clean up our retains
            for (ByteBuf chunk : chunks) {
                if (chunk.refCnt() > 0) {
                    chunk.release();
                }
            }
        } finally {
            cache.close();
        }
    }

    @Test
    public void testRetainReleaseSafety() throws Exception {
        ChunkCache cache = new ChunkCache(10 * ONE_MB, 60, ONE_MB);
        try {
            ChunkCache.ChunkKey key = new ChunkCache.ChunkKey("test-object", 0);
            ByteBuf chunk = cache.get(key, () -> createTestChunk((byte) 0x42));

            // Retain the chunk (simulating a reader holding a reference)
            chunk.retain();
            assertEquals(chunk.refCnt(), 2); // cache holds 1 + our retain = 2

            // Invalidate from cache — cache's removal listener releases its reference
            cache.close();
            assertEquals(chunk.refCnt(), 1); // only our retain remains

            // Chunk is still usable
            assertEquals(chunk.getByte(0), (byte) 0x42);

            // Release our reference — now fully released
            chunk.release();
            assertEquals(chunk.refCnt(), 0);
        } finally {
            // cache already closed above, but safe to call again
        }
    }

    @Test
    public void testInvalidateAll() throws Exception {
        ChunkCache cache = new ChunkCache(10 * ONE_MB, 60, ONE_MB);
        List<ByteBuf> chunks = new ArrayList<>();
        try {
            // Insert multiple chunks, retain each so we can inspect after close
            for (int i = 0; i < 5; i++) {
                ChunkCache.ChunkKey key = new ChunkCache.ChunkKey("test-object", (long) i * ONE_MB);
                byte fillByte = (byte) (i + 1);
                ByteBuf chunk = cache.get(key, () -> createTestChunk(fillByte));
                chunk.retain();
                chunks.add(chunk);
            }

            // Verify all are alive (cache + our retain = 2)
            for (ByteBuf chunk : chunks) {
                assertEquals(chunk.refCnt(), 2);
            }

            // Close cache — invalidateAll should release all cache references
            cache.close();

            // Verify each chunk's refCnt dropped by 1 (only our retain remains)
            for (ByteBuf chunk : chunks) {
                assertEquals(chunk.refCnt(), 1);
            }
        } finally {
            // Clean up our retains
            for (ByteBuf chunk : chunks) {
                if (chunk.refCnt() > 0) {
                    chunk.release();
                }
            }
        }
    }

    @Test
    public void testGetIfPresent() throws Exception {
        ChunkCache cache = new ChunkCache(10 * ONE_MB, 60, ONE_MB);
        try {
            ChunkCache.ChunkKey key = new ChunkCache.ChunkKey("test-object", 0);

            // Not yet loaded — should return null
            ByteBuf missing = cache.getIfPresent(key);
            assertEquals(missing, null);

            // Load it
            cache.get(key, () -> createTestChunk((byte) 0x01));

            // Now should be present
            ByteBuf present = cache.getIfPresent(key);
            assertNotNull(present);
            assertEquals(present.getByte(0), (byte) 0x01);
        } finally {
            cache.close();
        }
    }

    @Test
    public void testGetChunkSize() {
        ChunkCache cache = new ChunkCache(10 * ONE_MB, 60, ONE_MB);
        try {
            assertEquals(cache.getChunkSize(), ONE_MB);
        } finally {
            cache.close();
        }
    }
}
