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

import static org.apache.bookkeeper.client.api.BKException.Code.NoSuchLedgerExistsException;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.OffloadedLedgerMetadata;
import org.apache.bookkeeper.mledger.impl.LedgerOffloaderStatsImpl;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.JCloudBlobStoreProvider;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.apache.pulsar.common.naming.TopicName;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.io.Payloads;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class BlobStoreManagedLedgerOffloaderTest extends BlobStoreManagedLedgerOffloaderBase {

    private static final Logger log = LoggerFactory.getLogger(BlobStoreManagedLedgerOffloaderTest.class);
    private final ScheduledExecutorService scheduledExecutorService;
    private TieredStorageConfiguration mockedConfig;
    private final LedgerOffloaderStats offloaderStats;

    BlobStoreManagedLedgerOffloaderTest() throws Exception {
        super();
        config = getConfiguration(BUCKET);
        JCloudBlobStoreProvider provider = getBlobStoreProvider();
        assertNotNull(provider);
        provider.validate(config);
        blobStore = provider.getBlobStore(config);
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        this.offloaderStats = LedgerOffloaderStats.create(true, true, scheduledExecutorService, 60);
    }

    @AfterClass(alwaysRun = true)
    protected void cleanupInstance() throws Exception {
        offloaderStats.close();
        scheduledExecutorService.shutdownNow();
    }

    private BlobStoreManagedLedgerOffloader getOffloader() throws IOException {
        return getOffloader(BUCKET);
    }

    private BlobStoreManagedLedgerOffloader getOffloader(BlobStore mockedBlobStore) throws IOException {
        return getOffloader(BUCKET, mockedBlobStore);
    }

    private BlobStoreManagedLedgerOffloader getOffloader(String bucket) throws IOException {
        mockedConfig = mock(TieredStorageConfiguration.class, delegatesTo(getConfiguration(bucket)));
        Mockito.doReturn(blobStore).when(mockedConfig).getBlobStore(); // Use the REAL blobStore
        BlobStoreManagedLedgerOffloader offloader = BlobStoreManagedLedgerOffloader.create(mockedConfig,
                new HashMap<String, String>(), scheduler, scheduler, this.offloaderStats,
                entryOffsetsCache, null);
        return offloader;
    }

    private BlobStoreManagedLedgerOffloader getOffloader(String bucket, BlobStore mockedBlobStore) throws IOException {
        mockedConfig = mock(TieredStorageConfiguration.class, delegatesTo(getConfiguration(bucket)));
        Mockito.doReturn(mockedBlobStore).when(mockedConfig).getBlobStore();
        BlobStoreManagedLedgerOffloader offloader = BlobStoreManagedLedgerOffloader.create(mockedConfig,
                new HashMap<String, String>(), scheduler, scheduler, this.offloaderStats,
                entryOffsetsCache, null);
        return offloader;
    }

    @Test(timeOut = 600000)  // 10 minutes.
    public void testHappyCase() throws Exception {
        @Cleanup
        LedgerOffloader offloader = getOffloader();
        offloader.offload(buildReadHandle(), UUID.randomUUID(), new HashMap<>()).get();
    }

    @Test(timeOut = 600000)  // 10 minutes.
    public void testBucketDoesNotExist() throws Exception {

        if (provider == JCloudBlobStoreProvider.TRANSIENT) {
            // Skip this test, since it isn't applicable.
            return;
        }

        @Cleanup
        LedgerOffloader offloader = getOffloader("some-non-existant-bucket-name");
        try {
            offloader.offload(buildReadHandle(), UUID.randomUUID(), new HashMap<>()).get();
            Assert.fail("Shouldn't be able to add to bucket");
        } catch (ExecutionException e) {
            log.error("Exception: ", e);
            Assert.assertTrue(e.getMessage().toLowerCase().contains("not found"));
        }
    }

    @Test(timeOut = 600000)  // 10 minutes.
    public void testOffloadAndRead() throws Exception {
        @Cleanup
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 3);
        @Cleanup
        LedgerOffloader offloader = getOffloader();

        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        @Cleanup
        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());

        try (LedgerEntries toWriteEntries = toWrite.read(0, toWrite.getLastAddConfirmed());
             LedgerEntries toTestEntries = toTest.read(0, toTest.getLastAddConfirmed())) {
            Iterator<LedgerEntry> toWriteIter = toWriteEntries.iterator();
            Iterator<LedgerEntry> toTestIter = toTestEntries.iterator();

            while (toWriteIter.hasNext() && toTestIter.hasNext()) {
                LedgerEntry toWriteEntry = toWriteIter.next();
                LedgerEntry toTestEntry = toTestIter.next();

                assertEquals(toWriteEntry.getLedgerId(), toTestEntry.getLedgerId());
                assertEquals(toWriteEntry.getEntryId(), toTestEntry.getEntryId());
                assertEquals(toWriteEntry.getLength(), toTestEntry.getLength());
                assertEquals(toWriteEntry.getEntryBuffer(), toTestEntry.getEntryBuffer());
            }
            Assert.assertFalse(toWriteIter.hasNext());
            Assert.assertFalse(toTestIter.hasNext());
        }
    }

    @Test(timeOut = 60000)
    public void testReadHandlerState() throws Exception {
        @Cleanup
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 3);
        @Cleanup
        LedgerOffloader offloader = getOffloader();

        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        BlobStoreBackedReadHandleImpl toTest = (BlobStoreBackedReadHandleImpl) offloader
                .readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        Assert.assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());
        Assert.assertEquals(toTest.getState(), BlobStoreBackedReadHandleImpl.State.Opened);
        @Cleanup
        LedgerEntries ledgerEntries = toTest.read(0, 1);
        toTest.close();
        Assert.assertEquals(toTest.getState(), BlobStoreBackedReadHandleImpl.State.Closed);
    }

    @Test(timeOut = 600000)  // 10 minutes.
    public void testOffloadAndReadMetrics() throws Exception {
        @Cleanup
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 3);
        @Cleanup
        LedgerOffloader offloader = getOffloader();

        UUID uuid = UUID.randomUUID();
        String managedLegerName = "public/default/persistent/testOffload";
        String topic = TopicName.fromPersistenceNamingEncoding(managedLegerName);
        Map<String, String> extraMap = new HashMap<>();
        extraMap.put("ManagedLedgerName", managedLegerName);
        offloader.offload(toWrite, uuid, extraMap).get();

        @Cleanup
        LedgerOffloaderStatsImpl offloaderStats = (LedgerOffloaderStatsImpl) this.offloaderStats;

        assertEquals(offloaderStats.getOffloadError(topic), 0);
        assertTrue(offloaderStats.getOffloadBytes(topic) > 0);
        assertTrue(offloaderStats.getReadLedgerLatency(topic).count > 0);
        assertEquals(offloaderStats.getWriteStorageError(topic), 0);

        Map<String, String> map = new HashMap<>();
        map.putAll(offloader.getOffloadDriverMetadata());
        map.put("ManagedLedgerName", managedLegerName);
        @Cleanup
        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, map).get();
        @Cleanup
        LedgerEntries toTestEntries = toTest.read(0, toTest.getLastAddConfirmed());
        Iterator<LedgerEntry> toTestIter = toTestEntries.iterator();
        while (toTestIter.hasNext()) {
            LedgerEntry toTestEntry = toTestIter.next();
        }

        assertEquals(offloaderStats.getReadOffloadError(topic), 0);
        assertTrue(offloaderStats.getReadOffloadBytes(topic) > 0);
        assertTrue(offloaderStats.getReadOffloadDataLatency(topic).count > 0);
        assertTrue(offloaderStats.getReadOffloadIndexLatency(topic).count > 0);
    }

    @Test
    public void testOffloadFailInitDataBlockUpload() throws Exception {
        @Cleanup
        ReadHandle readHandle = buildReadHandle();
        UUID uuid = UUID.randomUUID();
        String failureString = "fail InitDataBlockUpload";

        // mock throw exception when initiateMultipartUpload
        try {
            BlobStore spiedBlobStore = mock(BlobStore.class, delegatesTo(blobStore));

            Mockito
                .doThrow(new RuntimeException(failureString))
                .when(spiedBlobStore).initiateMultipartUpload(any(), any(), any());

            @Cleanup
            BlobStoreManagedLedgerOffloader offloader = getOffloader(spiedBlobStore);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();
            Assert.fail("Should throw exception when initiateMultipartUpload");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(blobStore.blobExists(BUCKET,
                    DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertFalse(blobStore.blobExists(BUCKET,
                    DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));
        }
    }

    @Test
    public void testOffloadFailDataBlockPartUpload() throws Exception {
        @Cleanup
        ReadHandle readHandle = buildReadHandle();
        UUID uuid = UUID.randomUUID();
        String failureString = "fail DataBlockPartUpload";

        // mock throw exception when uploadPart
        try {

            BlobStore spiedBlobStore = mock(BlobStore.class, delegatesTo(blobStore));
            Mockito
                .doThrow(new RuntimeException(failureString))
                .when(spiedBlobStore).uploadMultipartPart(any(), anyInt(), any());

            @Cleanup
            BlobStoreManagedLedgerOffloader offloader = getOffloader(spiedBlobStore);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();
            Assert.fail("Should throw exception for when uploadPart");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(blobStore.blobExists(BUCKET,
                    DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertFalse(blobStore.blobExists(BUCKET,
                    DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));
        }
    }

    @Test
    public void testOffloadFailDataBlockUploadComplete() throws Exception {
        @Cleanup
        ReadHandle readHandle = buildReadHandle();
        UUID uuid = UUID.randomUUID();
        String failureString = "fail DataBlockUploadComplete";

        // mock throw exception when completeMultipartUpload
        try {
            BlobStore spiedBlobStore = mock(BlobStore.class, delegatesTo(blobStore));
            Mockito
                .doThrow(new RuntimeException(failureString))
                .when(spiedBlobStore).completeMultipartUpload(any(), any());
            Mockito
                .doNothing()
                .when(spiedBlobStore).abortMultipartUpload(any());

            @Cleanup
            BlobStoreManagedLedgerOffloader offloader = getOffloader(spiedBlobStore);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();

            Assert.fail("Should throw exception for when completeMultipartUpload");
        } catch (Exception e) {
            // excepted
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(blobStore.blobExists(BUCKET,
                    DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertFalse(blobStore.blobExists(BUCKET,
                    DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));
        }
    }

    @Test
    public void testOffloadFailPutIndexBlock() throws Exception {
        @Cleanup
        ReadHandle readHandle = buildReadHandle();
        UUID uuid = UUID.randomUUID();
        String failureString = "fail putObject";

        // mock throw exception when putObject
        try {
            BlobStore spiedBlobStore = mock(BlobStore.class, delegatesTo(blobStore));
            Mockito
                .doThrow(new RuntimeException(failureString))
                .when(spiedBlobStore).putBlob(any(), any());

            @Cleanup
            BlobStoreManagedLedgerOffloader offloader = getOffloader(spiedBlobStore);
            offloader.offload(readHandle, uuid, new HashMap<>()).get();

            Assert.fail("Should throw exception for when putObject for index block");
         } catch (Exception e) {
            // excepted
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            Assert.assertFalse(blobStore.blobExists(BUCKET,
                    DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertFalse(blobStore.blobExists(BUCKET,
                    DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));
        }
    }

    @Test(timeOut = 600000)
    public void testOffloadReadRandomAccess() throws Exception {
        @Cleanup
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 3);
        long[][] randomAccesses = new long[10][2];
        Random r = new Random(0);
        for (int i = 0; i < 10; i++) {
            long first = r.nextInt((int) toWrite.getLastAddConfirmed());
            long second = r.nextInt((int) toWrite.getLastAddConfirmed());
            if (second < first) {
                long tmp = first;
                first = second;
                second = tmp;
            }
            randomAccesses[i][0] = first;
            randomAccesses[i][1] = second;
        }

        @Cleanup
        LedgerOffloader offloader = getOffloader();

        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        @Cleanup
        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());

        for (long[] access : randomAccesses) {
            try (LedgerEntries toWriteEntries = toWrite.read(access[0], access[1]);
                 LedgerEntries toTestEntries = toTest.read(access[0], access[1])) {
                Iterator<LedgerEntry> toWriteIter = toWriteEntries.iterator();
                Iterator<LedgerEntry> toTestIter = toTestEntries.iterator();

                while (toWriteIter.hasNext() && toTestIter.hasNext()) {
                    LedgerEntry toWriteEntry = toWriteIter.next();
                    LedgerEntry toTestEntry = toTestIter.next();

                    assertEquals(toWriteEntry.getLedgerId(), toTestEntry.getLedgerId());
                    assertEquals(toWriteEntry.getEntryId(), toTestEntry.getEntryId());
                    assertEquals(toWriteEntry.getLength(), toTestEntry.getLength());
                    assertEquals(toWriteEntry.getEntryBuffer(), toTestEntry.getEntryBuffer());
                }
                Assert.assertFalse(toWriteIter.hasNext());
                Assert.assertFalse(toTestIter.hasNext());
            }
        }
    }

    @Test
    public void testOffloadReadInvalidEntryIds() throws Exception {
        @Cleanup
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
        @Cleanup
        LedgerOffloader offloader = getOffloader();
        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        @Cleanup
        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());

        try {
            toTest.read(-1, -1);
            Assert.fail("Shouldn't be able to read anything");
        } catch (BKException.BKIncorrectParameterException e) {
        }

        try {
            toTest.read(0, toTest.getLastAddConfirmed() + 1);
            Assert.fail("Shouldn't be able to read anything");
        } catch (BKException.BKIncorrectParameterException e) {
        }
    }

    @Test
    public void testDeleteOffloaded() throws Exception {
        @Cleanup
        ReadHandle readHandle = buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
        UUID uuid = UUID.randomUUID();

        @Cleanup
        BlobStoreManagedLedgerOffloader offloader = getOffloader();

        // verify object exist after offload
        offloader.offload(readHandle, uuid, new HashMap<>()).get();
        Assert.assertTrue(blobStore.blobExists(BUCKET,
                DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
        Assert.assertTrue(blobStore.blobExists(BUCKET,
                DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));

        // verify object deleted after delete
        offloader.deleteOffloaded(readHandle.getId(), uuid, config.getOffloadDriverMetadata()).get();
        Assert.assertFalse(blobStore.blobExists(BUCKET,
                DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
        Assert.assertFalse(blobStore.blobExists(BUCKET,
                DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));
    }

    @Test
    public void testDeleteOffloadedFail() throws Exception {
        String failureString = "fail deleteOffloaded";
        @Cleanup
        ReadHandle readHandle = buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
        UUID uuid = UUID.randomUUID();

        BlobStore spiedBlobStore = mock(BlobStore.class, delegatesTo(blobStore));

        Mockito
            .doThrow(new RuntimeException(failureString))
            .when(spiedBlobStore).removeBlobs(any(), any());

        @Cleanup
        BlobStoreManagedLedgerOffloader offloader = getOffloader(spiedBlobStore);

        try {
            // verify object exist after offload
            offloader.offload(readHandle, uuid, new HashMap<>()).get();
            Assert.assertTrue(blobStore.blobExists(BUCKET,
                    DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertTrue(blobStore.blobExists(BUCKET,
                    DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));

            offloader.deleteOffloaded(readHandle.getId(), uuid, config.getOffloadDriverMetadata()).get();
        } catch (Exception e) {
            // expected
            Assert.assertTrue(e.getCause().getMessage().contains(failureString));
            // verify object still there.
            Assert.assertTrue(blobStore.blobExists(BUCKET,
                    DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid)));
            Assert.assertTrue(blobStore.blobExists(BUCKET,
                    DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid)));
        }
    }

    @Test
    public void testOffloadEmpty() throws Exception {
        CompletableFuture<LedgerEntries> noEntries = new CompletableFuture<>();
        noEntries.completeExceptionally(new BKException.BKReadException());

        ReadHandle readHandle = Mockito.mock(ReadHandle.class);
        Mockito.doReturn(-1L).when(readHandle).getLastAddConfirmed();
        Mockito.doReturn(noEntries).when(readHandle).readAsync(anyLong(), anyLong());
        Mockito.doReturn(0L).when(readHandle).getLength();
        Mockito.doReturn(true).when(readHandle).isClosed();
        Mockito.doReturn(1234L).when(readHandle).getId();

        UUID uuid = UUID.randomUUID();
        @Cleanup
        LedgerOffloader offloader = getOffloader();

        try {
            offloader.offload(readHandle, uuid, new HashMap<>()).get();
            Assert.fail("Shouldn't have been able to offload");
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
    }

    @Test
    public void testReadUnknownDataVersion() throws Exception {
        @Cleanup
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
        BlobStoreManagedLedgerOffloader offloader = getOffloader();

        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        String dataKey = DataBlockUtils.dataBlockOffloadKey(toWrite.getId(), uuid);

        // Here it will return a Immutable map.
        Assert.assertTrue(blobStore.blobExists(BUCKET, dataKey));
        Map<String, String> immutableMap = blobStore.blobMetadata(BUCKET, dataKey).getUserMetadata();
        Map<String, String> userMeta = Maps.newHashMap();
        userMeta.putAll(immutableMap);
        userMeta.put(DataBlockUtils.METADATA_FORMAT_VERSION_KEY.toLowerCase(), String.valueOf(-12345));
        blobStore.copyBlob(BUCKET, dataKey, BUCKET, dataKey, CopyOptions.builder().userMetadata(userMeta).build());

        try (ReadHandle toRead = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get()) {
            toRead.readAsync(0, 0).get();
            Assert.fail("Shouldn't have been able to read");
        } catch (ExecutionException e) {
            log.error("Exception: ", e);
            assertEquals(e.getCause().getClass(), IOException.class);
            Assert.assertTrue(e.getCause().getMessage().contains("Error reading from BlobStore"));
        }

        userMeta.put(DataBlockUtils.METADATA_FORMAT_VERSION_KEY.toLowerCase(), String.valueOf(12345));
        blobStore.copyBlob(BUCKET, dataKey, BUCKET, dataKey, CopyOptions.builder().userMetadata(userMeta).build());

        try (ReadHandle toRead = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get()) {
            toRead.readAsync(0, 0).get();
            Assert.fail("Shouldn't have been able to read");
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), IOException.class);
            Assert.assertTrue(e.getCause().getMessage().contains("Error reading from BlobStore"));
        }
    }

    @Test
    public void testReadUnknownIndexVersion() throws Exception {
        @Cleanup
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
        BlobStoreManagedLedgerOffloader offloader = getOffloader();

        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        String indexKey = DataBlockUtils.indexBlockOffloadKey(toWrite.getId(), uuid);

        // Here it will return a Immutable map.
        Map<String, String> immutableMap = blobStore.blobMetadata(BUCKET, indexKey).getUserMetadata();
        Map<String, String> userMeta = Maps.newHashMap();
        userMeta.putAll(immutableMap);
        userMeta.put(DataBlockUtils.METADATA_FORMAT_VERSION_KEY.toLowerCase(), String.valueOf(-12345));
        blobStore.copyBlob(BUCKET, indexKey, BUCKET, indexKey, CopyOptions.builder().userMetadata(userMeta).build());

        try {
            offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
            Assert.fail("Shouldn't have been able to open");
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), IOException.class);
            Assert.assertTrue(e.getCause().getMessage().contains("Invalid object version"));
        }

        userMeta.put(DataBlockUtils.METADATA_FORMAT_VERSION_KEY.toLowerCase(), String.valueOf(12345));
        blobStore.copyBlob(BUCKET, indexKey, BUCKET, indexKey, CopyOptions.builder().userMetadata(userMeta).build());

        try {
            offloader.readOffloaded(toWrite.getId(), uuid, config.getOffloadDriverMetadata()).get();
            Assert.fail("Shouldn't have been able to open");
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), IOException.class);
            Assert.assertTrue(e.getCause().getMessage().contains("Invalid object version"));
        }
    }

    @Test
    public void testReadEOFException() throws Throwable {
        @Cleanup
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
        @Cleanup
        LedgerOffloader offloader = getOffloader();
        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        @Cleanup
        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        Assert.assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());
        @Cleanup
        LedgerEntries ledgerEntries = toTest.readAsync(0, toTest.getLastAddConfirmed()).get();

        try {
            @Cleanup
            LedgerEntries ledgerEntries2 = toTest.readAsync(0, 0).get();
        } catch (Exception e) {
            Assert.fail("Get unexpected exception when reading entries", e);
        }
    }

    @Test(timeOut = 600000)  // 10 minutes.
    public void testScanLedgers() throws Exception {
        @Cleanup
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 3);
        @Cleanup
        LedgerOffloader offloader = getOffloader();

        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        List<OffloadedLedgerMetadata> result = new ArrayList<>();
        offloader.scanLedgers(
                (m) -> {
                    log.info("found {}", m);
                    if (m.getLedgerId() == toWrite.getId()) {
                        result.add(m);
                    }
                    return true;
                }, offloader.getOffloadDriverMetadata());
        assertEquals(2, result.size());

        // data and index

        OffloadedLedgerMetadata offloadedLedgerMetadata = result.get(0);
        assertEquals(toWrite.getId(), offloadedLedgerMetadata.getLedgerId());

        OffloadedLedgerMetadata offloadedLedgerMetadata2 = result.get(1);
        assertEquals(toWrite.getId(), offloadedLedgerMetadata2.getLedgerId());
    }

    @Test
    public void testReadWithAClosedLedgerHandler() throws Exception {
        @Cleanup
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
        @Cleanup
        LedgerOffloader offloader = getOffloader();
        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        @Cleanup
        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        Assert.assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());
        long lac = toTest.getLastAddConfirmed();
        @Cleanup
        LedgerEntries ledgerEntries = toTest.readAsync(0, lac).get();
        toTest.closeAsync().get();
        try {
            toTest.readAsync(0, lac).get();
        } catch (Exception e) {
            if (e.getCause() instanceof ManagedLedgerException.OffloadReadHandleClosedException) {
                // expected exception
                return;
            }
            throw e;
        }
    }

    @Test
    public void testReadNotExistLedger() throws Exception {
        @Cleanup
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 3);
        @Cleanup
        LedgerOffloader offloader = getOffloader();

        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();
        @Cleanup
        ReadHandle offloadRead = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        assertEquals(offloadRead.getLastAddConfirmed(), toWrite.getLastAddConfirmed());

        // delete blob(ledger)
        blobStore.removeBlob(BUCKET, DataBlockUtils.dataBlockOffloadKey(toWrite.getId(), uuid));

        try {
            offloadRead.read(0, offloadRead.getLastAddConfirmed());
            fail("Should be read fail");
        } catch (BKException e) {
            assertEquals(e.getCode(), NoSuchLedgerExistsException);
        }
    }

    // ---- Helper for integration tests that need the new ChunkCache read path ----

    private BlobStoreManagedLedgerOffloader getOffloaderWithChunkCache(int chunkSize) throws IOException {
        mockedConfig = mock(TieredStorageConfiguration.class, delegatesTo(getConfiguration(BUCKET)));
        Mockito.doReturn(blobStore).when(mockedConfig).getBlobStore();
        // Create a ChunkCache with generous limits for testing
        ChunkCache cache = new ChunkCache(
                64 * 1024 * 1024, // 64 MB max cache
                300,              // 5 minutes TTL
                chunkSize);
        BlobStoreManagedLedgerOffloader offloader = BlobStoreManagedLedgerOffloader.create(mockedConfig,
                new HashMap<String, String>(), scheduler, scheduler, this.offloaderStats,
                entryOffsetsCache, cache);
        return offloader;
    }

    // ---- End-to-end integration tests for the new cache design ----

    /**
     * Test the full offload-then-read cycle with per-entry index and ChunkCache.
     *
     * 1. Offloads a ledger (writes per-entry offsets into the index).
     * 2. Opens a read handle with a real ChunkCache (enables the new read path).
     * 3. Reads all entries and verifies they match the original data.
     *
     * This validates: write per-entry offsets -> read index -> bulk-load OffsetsCache
     *   -> read entries via chunk cache path.
     */
    @Test(timeOut = 600000)
    public void testOffloadAndReadWithPerEntryIndex() throws Exception {
        // Use a chunkSize large enough to hold multiple entries so that
        // the read path goes through the chunk cache rather than direct fetch.
        int chunkSize = 1 * 1024 * 1024; // 1 MB
        @Cleanup
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 3);
        @Cleanup
        LedgerOffloader offloader = getOffloaderWithChunkCache(chunkSize);

        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        // Verify that per-entry offsets were bulk-loaded into OffsetsCache during index read
        @Cleanup
        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());

        // The OffsetsCache should now contain entries from the per-entry index
        // (bulk-loaded during fromStream). Verify a sample entry is cached.
        Long cachedOffset = entryOffsetsCache.getIfPresent(toWrite.getId(), 0);
        assertNotNull(cachedOffset, "Entry 0 offset should have been bulk-loaded into OffsetsCache");

        // Read all entries and verify data correctness
        try (LedgerEntries toWriteEntries = toWrite.read(0, toWrite.getLastAddConfirmed());
             LedgerEntries toTestEntries = toTest.read(0, toTest.getLastAddConfirmed())) {
            Iterator<LedgerEntry> toWriteIter = toWriteEntries.iterator();
            Iterator<LedgerEntry> toTestIter = toTestEntries.iterator();

            while (toWriteIter.hasNext() && toTestIter.hasNext()) {
                LedgerEntry toWriteEntry = toWriteIter.next();
                LedgerEntry toTestEntry = toTestIter.next();

                assertEquals(toWriteEntry.getLedgerId(), toTestEntry.getLedgerId());
                assertEquals(toWriteEntry.getEntryId(), toTestEntry.getEntryId());
                assertEquals(toWriteEntry.getLength(), toTestEntry.getLength());
                assertEquals(toWriteEntry.getEntryBuffer(), toTestEntry.getEntryBuffer());
            }
            Assert.assertFalse(toWriteIter.hasNext());
            Assert.assertFalse(toTestIter.hasNext());
        }

        // Also verify random access reads work correctly with the new path
        long mid = toWrite.getLastAddConfirmed() / 2;
        try (LedgerEntries toWriteEntries = toWrite.read(mid, mid + 1);
             LedgerEntries toTestEntries = toTest.read(mid, mid + 1)) {
            Iterator<LedgerEntry> toWriteIter = toWriteEntries.iterator();
            Iterator<LedgerEntry> toTestIter = toTestEntries.iterator();

            while (toWriteIter.hasNext() && toTestIter.hasNext()) {
                LedgerEntry toWriteEntry = toWriteIter.next();
                LedgerEntry toTestEntry = toTestIter.next();

                assertEquals(toWriteEntry.getLedgerId(), toTestEntry.getLedgerId());
                assertEquals(toWriteEntry.getEntryId(), toTestEntry.getEntryId());
                assertEquals(toWriteEntry.getEntryBuffer(), toTestEntry.getEntryBuffer());
            }
            Assert.assertFalse(toWriteIter.hasNext());
            Assert.assertFalse(toTestIter.hasNext());
        }
    }

    /**
     * Test backward compatibility: read old-format index (without per-entry offsets)
     * using the new code path with ChunkCache.
     *
     * 1. Offloads data normally (producing data blocks in the blob store).
     * 2. Replaces the index blob with one that has NO per-entry offsets (old format).
     * 3. Opens a read handle with the new code (ChunkCache enabled).
     * 4. Reads entries — the new code must fall back to scanning through ChunkCache.
     * 5. Verifies all entries match.
     */
    @Test(timeOut = 600000)
    public void testReadOldFormatIndexWithNewCode() throws Exception {
        int chunkSize = 1 * 1024 * 1024; // 1 MB
        @Cleanup
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
        @Cleanup
        BlobStoreManagedLedgerOffloader offloader = getOffloaderWithChunkCache(chunkSize);

        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        // Now read the current index blob, rebuild it WITHOUT per-entry offsets,
        // and replace it in the blob store.
        String indexKey = DataBlockUtils.indexBlockOffloadKey(toWrite.getId(), uuid);

        // Build an old-format index that has only block-level entries (no per-entry offsets).
        // We do this by reading the existing index, then rebuilding without calling addEntryOffset().
        // First, read the existing index to get block-level information.
        @Cleanup("close")
        ReadHandle tempRead = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        BlobStoreBackedReadHandleImpl tempImpl = (BlobStoreBackedReadHandleImpl) tempRead;
        long lac = tempRead.getLastAddConfirmed();

        // Build a fresh index with only block-level entries (mimicking old format)
        OffloadIndexBlockBuilder oldFormatBuilder = OffloadIndexBlockBuilder.create()
                .withLedgerMetadata(toWrite.getLedgerMetadata())
                .withDataBlockHeaderLength(BlockAwareSegmentInputStreamImpl.getHeaderSize());

        // Re-read data to figure out block boundaries.
        // We know the original offload used DEFAULT_BLOCK_SIZE with 1 block.
        // Reconstruct by computing the data object length from the offloaded data.
        Blob dataBlob = blobStore.getBlob(BUCKET, DataBlockUtils.dataBlockOffloadKey(toWrite.getId(), uuid));
        long dataObjectLength = dataBlob.getMetadata().getContentMetadata().getContentLength();

        oldFormatBuilder.addBlock(0, 1, (int) dataObjectLength);
        // Intentionally NOT calling addEntryOffset() — this simulates the old format
        OffloadIndexBlock oldFormatIndex = oldFormatBuilder.withDataObjectLength(dataObjectLength).build();
        OffloadIndexBlock.IndexInputStream oldIndexStream = oldFormatIndex.toStream();

        // Read out the old-format index bytes
        byte[] oldIndexBytes;
        try (InputStream is = oldIndexStream) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int n;
            while ((n = is.read(buf)) != -1) {
                baos.write(buf, 0, n);
            }
            oldIndexBytes = baos.toByteArray();
        }
        oldFormatIndex.close();

        // Replace the index blob in the store with the old-format version
        Map<String, String> existingMeta = new HashMap<>(
                blobStore.blobMetadata(BUCKET, indexKey).getUserMetadata());
        // Remove version key since addVersionInfo will re-add it (avoids duplicate key in ImmutableMap)
        existingMeta.remove(DataBlockUtils.METADATA_FORMAT_VERSION_KEY.toLowerCase());
        BlobBuilder blobBuilder = blobStore.blobBuilder(indexKey);
        DataBlockUtils.addVersionInfo(blobBuilder, existingMeta);
        Blob replacementBlob = blobBuilder
                .payload(Payloads.newByteArrayPayload(oldIndexBytes))
                .contentLength((long) oldIndexBytes.length)
                .build();
        blobStore.putBlob(BUCKET, replacementBlob);

        // Clear the offsets cache so the new read path cannot use previously cached offsets
        entryOffsetsCache.clear();

        // Now open a read handle with the new code — should fall back to scan path
        @Cleanup
        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());

        // Read all entries and verify correctness (scan-through-chunk-cache fallback)
        try (LedgerEntries toWriteEntries = toWrite.read(0, toWrite.getLastAddConfirmed());
             LedgerEntries toTestEntries = toTest.read(0, toTest.getLastAddConfirmed())) {
            Iterator<LedgerEntry> toWriteIter = toWriteEntries.iterator();
            Iterator<LedgerEntry> toTestIter = toTestEntries.iterator();

            while (toWriteIter.hasNext() && toTestIter.hasNext()) {
                LedgerEntry toWriteEntry = toWriteIter.next();
                LedgerEntry toTestEntry = toTestIter.next();

                assertEquals(toWriteEntry.getLedgerId(), toTestEntry.getLedgerId());
                assertEquals(toWriteEntry.getEntryId(), toTestEntry.getEntryId());
                assertEquals(toWriteEntry.getLength(), toTestEntry.getLength());
                assertEquals(toWriteEntry.getEntryBuffer(), toTestEntry.getEntryBuffer());
            }
            Assert.assertFalse(toWriteIter.hasNext());
            Assert.assertFalse(toTestIter.hasNext());
        }
    }

    /**
     * Test that large reads bypass the ChunkCache and use direct fetch instead.
     *
     * 1. Offloads a ledger with many entries spanning multiple blocks.
     * 2. Configures a small ChunkCache chunkSize (e.g. 64KB) so that reading a
     *    range of entries whose total size exceeds the chunkSize triggers the direct fetch path.
     * 3. Reads a range of entries and verifies all data matches.
     */
    @Test(timeOut = 600000)
    public void testLargeReadBypassesChunkCache() throws Exception {
        // Use a very small chunk size so that reading many entries will exceed it
        // and trigger the direct-fetch path (totalBytes > chunkSize).
        int smallChunkSize = 64 * 1024; // 64 KB
        @Cleanup
        ReadHandle toWrite = buildReadHandle(DEFAULT_BLOCK_SIZE, 3);
        @Cleanup
        LedgerOffloader offloader = getOffloaderWithChunkCache(smallChunkSize);

        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, new HashMap<>()).get();

        @Cleanup
        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, Collections.emptyMap()).get();
        assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());

        // Read a large range that spans many entries — total bytes should exceed 64KB chunkSize,
        // forcing the direct fetch path.
        long lastEntry = toWrite.getLastAddConfirmed();
        assertTrue(lastEntry > 10, "Expected more than 10 entries for this test to be meaningful");

        try (LedgerEntries toWriteEntries = toWrite.read(0, lastEntry);
             LedgerEntries toTestEntries = toTest.read(0, lastEntry)) {
            Iterator<LedgerEntry> toWriteIter = toWriteEntries.iterator();
            Iterator<LedgerEntry> toTestIter = toTestEntries.iterator();

            int count = 0;
            while (toWriteIter.hasNext() && toTestIter.hasNext()) {
                LedgerEntry toWriteEntry = toWriteIter.next();
                LedgerEntry toTestEntry = toTestIter.next();

                assertEquals(toWriteEntry.getLedgerId(), toTestEntry.getLedgerId());
                assertEquals(toWriteEntry.getEntryId(), toTestEntry.getEntryId());
                assertEquals(toWriteEntry.getLength(), toTestEntry.getLength());
                assertEquals(toWriteEntry.getEntryBuffer(), toTestEntry.getEntryBuffer());
                count++;
            }
            Assert.assertFalse(toWriteIter.hasNext());
            Assert.assertFalse(toTestIter.hasNext());
            assertEquals(count, lastEntry + 1);
        }

        // Also verify that reading a small range (few entries) still works
        // — this will go through the chunk cache path since total bytes < chunkSize.
        try (LedgerEntries toWriteEntries = toWrite.read(0, 0);
             LedgerEntries toTestEntries = toTest.read(0, 0)) {
            Iterator<LedgerEntry> toWriteIter = toWriteEntries.iterator();
            Iterator<LedgerEntry> toTestIter = toTestEntries.iterator();

            LedgerEntry toWriteEntry = toWriteIter.next();
            LedgerEntry toTestEntry = toTestIter.next();
            assertEquals(toWriteEntry.getEntryBuffer(), toTestEntry.getEntryBuffer());
        }
    }
}
