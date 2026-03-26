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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.BackedInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.net.BookieId;
import org.apache.commons.lang3.tuple.Pair;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.io.Payload;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BlobStoreBackedReadHandleImplTest {

    private OffsetsCache offsetsCache = new OffsetsCache();

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

    @AfterClass
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (offsetsCache != null) {
            offsetsCache.close();
        }
    }

    @AfterClass
    public void clearCache() throws Exception {
        offsetsCache.clear();
    }

    private String getExpectedEntryContent(int entryId) {
        return "Entry " + entryId;
    }

    private Pair<BlobStoreBackedReadHandleImpl, ByteBuf> createReadHandle(
            long ledgerId, int entries, boolean hasDirtyData) throws Exception {
        // Build data.
        List<Pair<Integer, Integer>> offsets = new ArrayList<>();
        int totalLen = 0;
        ByteBuf data = ByteBufAllocator.DEFAULT.heapBuffer(1024);
        data.writeInt(0);
        data.writerIndex(128);
        //data.readerIndex(128);
        for (int i = 0; i < entries; i++) {
            if (hasDirtyData && i == 1) {
                data.writeBytes("dirty data".getBytes(UTF_8));
            }
            offsets.add(Pair.of(i, data.writerIndex()));
            offsetsCache.put(ledgerId, i, data.writerIndex());
            byte[] entryContent = getExpectedEntryContent(i).getBytes(UTF_8);
            totalLen += entryContent.length;
            data.writeInt(entryContent.length);
            data.writeLong(i);
            data.writeBytes(entryContent);
        }
        // Build metadata.
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
                .withId(ledgerId)
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withDigestType(DigestType.CRC32C)
                .withPassword("pwd".getBytes(UTF_8))
                .withClosedState()
                .withLastEntryId(entries)
                .withLength(totalLen)
                .newEnsembleEntry(0L, Arrays.asList(BookieId.parse("127.0.0.1:3181")))
                .build();
        BackedInputStreamImpl inputStream = new BackedInputStreamImpl(data);
        // Since we have written data to "offsetsCache", the index will never be used.
        OffloadIndexBlock mockIndex = mock(OffloadIndexBlock.class);
        when(mockIndex.getLedgerMetadata()).thenReturn(metadata);
        for (Pair<Integer, Integer> pair : offsets) {
            when(mockIndex.getIndexEntryForEntry(pair.getLeft())).thenReturn(
                    OffloadIndexEntryImpl.of(pair.getLeft(), 0, pair.getRight(), 0));
        }
        // Build obj.
        return Pair.of(new BlobStoreBackedReadHandleImpl(ledgerId, mockIndex, inputStream, executor, offsetsCache,
                null), data);
    }

    /**
     * Build a data ByteBuf and a read handle that uses the new ChunkCache + direct fetch read path.
     * The data starts at offset 0 with no header prefix (entries start at offset 0).
     */
    private Pair<BlobStoreBackedReadHandleImpl, ByteBuf> createChunkCacheReadHandle(
            long ledgerId, int entries, int chunkSize) throws Exception {
        // Build data: entries packed sequentially starting at offset 0.
        List<Pair<Integer, Integer>> offsets = new ArrayList<>();
        int totalLen = 0;
        ByteBuf data = ByteBufAllocator.DEFAULT.heapBuffer(4096);
        for (int i = 0; i < entries; i++) {
            offsets.add(Pair.of(i, data.writerIndex()));
            byte[] entryContent = getExpectedEntryContent(i).getBytes(UTF_8);
            totalLen += entryContent.length;
            data.writeInt(entryContent.length);
            data.writeLong(i);
            data.writeBytes(entryContent);
        }

        int dataLen = data.writerIndex();
        // Copy data to a byte array for the mock blob store
        byte[] dataBytes = new byte[dataLen];
        data.getBytes(0, dataBytes);

        // Build metadata
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
                .withId(ledgerId)
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withDigestType(DigestType.CRC32C)
                .withPassword("pwd".getBytes(UTF_8))
                .withClosedState()
                .withLastEntryId(entries - 1)
                .withLength(totalLen)
                .newEnsembleEntry(0L, Arrays.asList(BookieId.parse("127.0.0.1:3181")))
                .build();

        // Mock index block — each entry has a precise index entry
        OffloadIndexBlock mockIndex = mock(OffloadIndexBlock.class);
        when(mockIndex.getLedgerMetadata()).thenReturn(metadata);
        when(mockIndex.getDataObjectLength()).thenReturn((long) dataLen);
        for (Pair<Integer, Integer> pair : offsets) {
            when(mockIndex.getIndexEntryForEntry(pair.getLeft())).thenReturn(
                    OffloadIndexEntryImpl.of(pair.getLeft(), 0, pair.getRight(), 0));
        }

        // Create a fresh offsets cache for each test to avoid cross-test pollution
        OffsetsCache testOffsetsCache = new OffsetsCache();
        // Pre-populate offsets cache with known offsets
        for (Pair<Integer, Integer> pair : offsets) {
            testOffsetsCache.put(ledgerId, pair.getLeft(), pair.getRight());
        }

        // Mock BlobStore that serves range requests from the byte array
        BlobStore mockBlobStore = createMockBlobStore(dataBytes);

        // Create ChunkCache with the specified chunk size
        ChunkCache chunkCache = new ChunkCache(
                10 * 1024 * 1024, // 10MB max
                60, // 60s TTL
                chunkSize);

        // Use a dummy BackedInputStream (not used in new read path but needed for constructor/close)
        BackedInputStreamImpl dummyStream = new BackedInputStreamImpl(
                ByteBufAllocator.DEFAULT.heapBuffer(1));

        String bucket = "test-bucket";
        String dataObjectKey = "test-data-key";

        BlobStoreBackedReadHandleImpl readHandle = new BlobStoreBackedReadHandleImpl(
                ledgerId, mockIndex, dummyStream, executor, testOffsetsCache, chunkCache,
                mockBlobStore, bucket, dataObjectKey, dataLen, true);

        return Pair.of(readHandle, data);
    }

    /**
     * Create a mock BlobStore that serves range requests from a byte array.
     */
    private BlobStore createMockBlobStore(byte[] dataBytes) throws IOException {
        BlobStore mockBlobStore = mock(BlobStore.class);
        when(mockBlobStore.getBlob(anyString(), anyString(), any(GetOptions.class)))
                .thenAnswer(new Answer<Blob>() {
                    @Override
                    public Blob answer(InvocationOnMock invocation) throws Throwable {
                        GetOptions options = invocation.getArgument(2);
                        // Extract range from GetOptions via its string representation
                        // GetOptions.range(start, end) sets the Range header
                        // We parse the range from the options
                        long[] range = extractRange(options);
                        long startRange = range[0];
                        long endRange = range[1];
                        int len = (int) (endRange - startRange + 1);
                        byte[] slice = new byte[len];
                        System.arraycopy(dataBytes, (int) startRange, slice, 0, len);

                        Blob mockBlob = mock(Blob.class);
                        Payload mockPayload = mock(Payload.class);
                        when(mockBlob.getPayload()).thenReturn(mockPayload);
                        when(mockPayload.openStream())
                                .thenReturn(new ByteArrayInputStream(slice));
                        return mockBlob;
                    }
                });
        return mockBlobStore;
    }

    /**
     * Extract the range [start, end] from GetOptions.
     * GetOptions.getRanges() returns strings in the format "start-end".
     */
    private long[] extractRange(GetOptions options) {
        List<String> ranges = options.getRanges();
        String rangeStr = ranges.get(0);
        String[] parts = rangeStr.split("-");
        return new long[]{Long.parseLong(parts[0]), Long.parseLong(parts[1])};
    }

    private static class BackedInputStreamImpl extends BackedInputStream {

        private ByteBuf data;

        private BackedInputStreamImpl(ByteBuf data){
            this.data = data;
        }

        @Override
        public void seek(long position) {
            data.readerIndex((int) position);
        }

        @Override
        public void seekForward(long position) throws IOException {
            data.readerIndex((int) position);
        }

        @Override
        public long getCurrentPosition() {
            return data.readerIndex();
        }

        @Override
        public int read() throws IOException {
            if (data.readableBytes() == 0) {
                throw new EOFException("The input-stream has no bytes to read");
            }
            return data.readByte();
        }

        @Override
        public int available() throws IOException {
            return data.readableBytes();
        }
    }

    @DataProvider
    public Object[][] streamStartAt() {
        return new Object[][] {
            // It gives a 0 value of the entry length.
            { 0, false },
            // It gives a 0 value of the entry length.
            { 1, false },
            // The first entry starts at 128.
            { 128, false },
            // It gives a 0 value of the entry length.
            { 0, true },
            // It gives a 0 value of the entry length.
            { 1, true },
            // The first entry starts at 128.
            { 128, true }
        };
    }

    @Test(dataProvider = "streamStartAt")
    public void testRead(int streamStartAt, boolean hasDirtyData) throws Exception {
        int entryCount = 5;
        Pair<BlobStoreBackedReadHandleImpl, ByteBuf> ledgerDataPair =
                createReadHandle(1, entryCount, hasDirtyData);
        BlobStoreBackedReadHandleImpl ledger = ledgerDataPair.getLeft();
        ByteBuf data = ledgerDataPair.getRight();
        data.readerIndex(streamStartAt);
        // Teat read each entry.
        for (int i = 0; i < 5; i++) {
            LedgerEntries entries = ledger.read(i, i);
            assertEquals(new String(entries.iterator().next().getEntryBytes()), getExpectedEntryContent(i));
        }
        // Test read all entries.
        LedgerEntries entries1 = ledger.read(0, entryCount - 1);
        Iterator<LedgerEntry> iterator1 = entries1.iterator();
        for (int i = 0; i < entryCount; i++) {
            assertEquals(new String(iterator1.next().getEntryBytes()), getExpectedEntryContent(i));
        }
        // Test a special case.
        // 1. Read from 0 to "lac - 1".
        // 2. Any reading.
        LedgerEntries entries2 = ledger.read(0, entryCount - 2);
        Iterator<LedgerEntry> iterator2 = entries2.iterator();
        for (int i = 0; i < entryCount - 1; i++) {
            assertEquals(new String(iterator2.next().getEntryBytes()), getExpectedEntryContent(i));
        }
        LedgerEntries entries3 = ledger.read(0, entryCount - 1);
        Iterator<LedgerEntry> iterator3 = entries3.iterator();
        for (int i = 0; i < entryCount; i++) {
            assertEquals(new String(iterator3.next().getEntryBytes()), getExpectedEntryContent(i));
        }
        // cleanup.
        ledger.close();
    }

    @Test
    public void testReadSingleEntryViaChunkCache() throws Exception {
        // Use a large chunk size (1MB) so the read goes through the chunk cache path
        int chunkSize = 1024 * 1024;
        int entryCount = 5;
        long ledgerId = 100;

        Pair<BlobStoreBackedReadHandleImpl, ByteBuf> pair =
                createChunkCacheReadHandle(ledgerId, entryCount, chunkSize);
        BlobStoreBackedReadHandleImpl ledger = pair.getLeft();

        // Read each entry individually — each should go through chunk cache path
        // since a single entry is much smaller than 1MB
        for (int i = 0; i < entryCount; i++) {
            LedgerEntries entries = ledger.read(i, i);
            LedgerEntry entry = entries.iterator().next();
            assertEquals(new String(entry.getEntryBytes()), getExpectedEntryContent(i));
        }

        ledger.close();
    }

    @Test
    public void testReadMultipleEntriesViaChunkCache() throws Exception {
        // Use a large chunk size so all entries fit in a single chunk read
        int chunkSize = 1024 * 1024;
        int entryCount = 10;
        long ledgerId = 101;

        Pair<BlobStoreBackedReadHandleImpl, ByteBuf> pair =
                createChunkCacheReadHandle(ledgerId, entryCount, chunkSize);
        BlobStoreBackedReadHandleImpl ledger = pair.getLeft();

        // Read all entries at once
        LedgerEntries entries = ledger.read(0, entryCount - 1);
        Iterator<LedgerEntry> iterator = entries.iterator();
        for (int i = 0; i < entryCount; i++) {
            LedgerEntry entry = iterator.next();
            assertEquals(entry.getEntryId(), i);
            assertEquals(new String(entry.getEntryBytes()), getExpectedEntryContent(i));
        }

        ledger.close();
    }

    @Test
    public void testReadEntriesDirectFetch() throws Exception {
        // Use a very small chunk size (32 bytes) so the total data exceeds it,
        // forcing the direct fetch path
        int chunkSize = 32;
        int entryCount = 5;
        long ledgerId = 102;

        Pair<BlobStoreBackedReadHandleImpl, ByteBuf> pair =
                createChunkCacheReadHandle(ledgerId, entryCount, chunkSize);
        BlobStoreBackedReadHandleImpl ledger = pair.getLeft();

        // Read all entries — total data should exceed 32 bytes, triggering direct fetch
        LedgerEntries entries = ledger.read(0, entryCount - 1);
        Iterator<LedgerEntry> iterator = entries.iterator();
        for (int i = 0; i < entryCount; i++) {
            LedgerEntry entry = iterator.next();
            assertEquals(entry.getEntryId(), i);
            assertEquals(new String(entry.getEntryBytes()), getExpectedEntryContent(i));
        }

        ledger.close();
    }

    @Test
    public void testReadSubsetOfEntries() throws Exception {
        int chunkSize = 1024 * 1024;
        int entryCount = 10;
        long ledgerId = 103;

        Pair<BlobStoreBackedReadHandleImpl, ByteBuf> pair =
                createChunkCacheReadHandle(ledgerId, entryCount, chunkSize);
        BlobStoreBackedReadHandleImpl ledger = pair.getLeft();

        // Read a subset of entries (3-7)
        LedgerEntries entries = ledger.read(3, 7);
        Iterator<LedgerEntry> iterator = entries.iterator();
        for (int i = 3; i <= 7; i++) {
            LedgerEntry entry = iterator.next();
            assertEquals(entry.getEntryId(), i);
            assertEquals(new String(entry.getEntryBytes()), getExpectedEntryContent(i));
        }

        ledger.close();
    }

    @Test
    public void testReadCrossChunkBoundary() throws Exception {
        // Use a small chunk size that will cause entries to span chunk boundaries.
        // Each entry is ~19 bytes ("Entry X" = 7 bytes + 12 byte header = 19 bytes).
        // With chunk size 30, some reads will cross chunk boundaries.
        int chunkSize = 30;
        int entryCount = 5;
        long ledgerId = 104;

        Pair<BlobStoreBackedReadHandleImpl, ByteBuf> pair =
                createChunkCacheReadHandle(ledgerId, entryCount, chunkSize);
        BlobStoreBackedReadHandleImpl ledger = pair.getLeft();

        // Read entries one by one — some may cross chunk boundaries
        for (int i = 0; i < entryCount; i++) {
            LedgerEntries entries = ledger.read(i, i);
            LedgerEntry entry = entries.iterator().next();
            assertEquals(entry.getEntryId(), i);
            assertEquals(new String(entry.getEntryBytes()), getExpectedEntryContent(i));
        }

        ledger.close();
    }

    @Test
    public void testReadRepeatedlyUsesCachedChunks() throws Exception {
        // Verify that reading the same entries multiple times works (chunk cache is hit)
        int chunkSize = 1024 * 1024;
        int entryCount = 5;
        long ledgerId = 105;

        Pair<BlobStoreBackedReadHandleImpl, ByteBuf> pair =
                createChunkCacheReadHandle(ledgerId, entryCount, chunkSize);
        BlobStoreBackedReadHandleImpl ledger = pair.getLeft();

        // Read all entries three times
        for (int round = 0; round < 3; round++) {
            LedgerEntries entries = ledger.read(0, entryCount - 1);
            Iterator<LedgerEntry> iterator = entries.iterator();
            for (int i = 0; i < entryCount; i++) {
                LedgerEntry entry = iterator.next();
                assertEquals(entry.getEntryId(), i);
                assertEquals(new String(entry.getEntryBytes()), getExpectedEntryContent(i));
            }
        }

        ledger.close();
    }
}
