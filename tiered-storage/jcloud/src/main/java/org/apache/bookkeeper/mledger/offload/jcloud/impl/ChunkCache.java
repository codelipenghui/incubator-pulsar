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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ChunkCache implements Closeable {

    public record ChunkKey(String dataObjectKey, long chunkStartOffset) {}

    private final Cache<ChunkKey, ByteBuf> cache;
    private final int chunkSize;

    public ChunkCache(long maxSizeBytes, long ttlSeconds, int chunkSize) {
        this.chunkSize = chunkSize;
        this.cache = CacheBuilder.newBuilder()
                .maximumWeight(maxSizeBytes)
                .weigher((ChunkKey key, ByteBuf buf) -> buf.readableBytes())
                .expireAfterWrite(ttlSeconds, TimeUnit.SECONDS)
                .removalListener((RemovalListener<ChunkKey, ByteBuf>) notification -> {
                    ByteBuf buf = notification.getValue();
                    if (buf != null && buf.refCnt() > 0) {
                        buf.release();
                    }
                })
                .build();
    }

    public static ChunkCache createDefault(int chunkSize) {
        long maxDirectMemory = PlatformDependent.maxDirectMemory();
        long defaultSize = (long) (maxDirectMemory * 0.15);
        return new ChunkCache(defaultSize, 60, chunkSize);
    }

    public ByteBuf get(ChunkKey key, Callable<ByteBuf> loader) throws ExecutionException {
        return cache.get(key, loader);
    }

    public ByteBuf getIfPresent(ChunkKey key) {
        return cache.getIfPresent(key);
    }

    public int getChunkSize() {
        return chunkSize;
    }

    @Override
    public void close() {
        cache.invalidateAll();
    }
}
