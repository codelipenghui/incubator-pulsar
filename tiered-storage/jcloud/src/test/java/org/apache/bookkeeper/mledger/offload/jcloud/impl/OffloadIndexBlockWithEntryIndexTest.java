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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexEntry;
import org.testng.annotations.Test;

public class OffloadIndexBlockWithEntryIndexTest {

    @Test
    public void testToStreamWithPerEntryIndex() throws Exception {
        LedgerMetadata metadata = OffloadIndexTest.createLedgerMetadata(12345L);

        OffloadIndexBlockBuilder builder = OffloadIndexBlockBuilder.create()
            .withLedgerMetadata(metadata)
            .withDataBlockHeaderLength(128);

        builder.addBlock(0, 1, 1000);
        builder.addEntryOffset(0, 128);
        builder.addEntryOffset(1, 240);
        builder.addEntryOffset(2, 352);

        builder.addBlock(3, 2, 1000);
        builder.addEntryOffset(3, 1128);
        builder.addEntryOffset(4, 1240);

        OffloadIndexBlock index = builder.withDataObjectLength(2000).build();
        OffloadIndexBlock.IndexInputStream stream = index.toStream();
        byte[] serialized = stream.readAllBytes();

        // Verify stream size matches actual length
        assertEquals(stream.getStreamSize(), serialized.length);

        // Verify block-level index still works via round-trip
        OffloadIndexBlock restored = (OffloadIndexBlock) OffloadIndexBlockBuilder.create()
            .fromStream(new ByteArrayInputStream(serialized));
        assertEquals(restored.getEntryCount(), 2);

        // Verify per-entry directory magic is at the right position
        // Parse the serialized bytes to find directory magic 0xE1D3E1D3
        // after the existing block entries
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serialized));
        dis.readInt(); // magic 0xDE47DE47
        dis.readInt(); // indexBlockLen (ignored)
        dis.readLong(); // dataObjectLen
        dis.readLong(); // dataHeaderLen
        int entryCount = dis.readInt(); // block count
        int metaLen = dis.readInt();
        dis.skipBytes(metaLen); // metadata
        dis.skipBytes(entryCount * 20); // block entries

        int directoryMagic = dis.readInt();
        assertEquals(directoryMagic, 0xE1D3E1D3);

        // Read directory -- 2 blocks x 20 bytes each
        for (int i = 0; i < 2; i++) {
            long firstEntryId = dis.readLong();
            long sectionOffset = dis.readLong();
            int sectionLength = dis.readInt();
            assertTrue(sectionOffset > 0);
            assertTrue(sectionLength > 0);
        }

        // Read per-entry sections
        // Block 0: 3 entries
        int block0Count = dis.readInt();
        assertEquals(block0Count, 3);
        assertEquals(dis.readLong(), 0L); // entryId 0
        assertEquals(dis.readLong(), 128L); // offset
        assertEquals(dis.readLong(), 1L); // entryId 1
        assertEquals(dis.readLong(), 240L);
        assertEquals(dis.readLong(), 2L); // entryId 2
        assertEquals(dis.readLong(), 352L);

        // Block 1: 2 entries
        int block1Count = dis.readInt();
        assertEquals(block1Count, 2);
        assertEquals(dis.readLong(), 3L);
        assertEquals(dis.readLong(), 1128L);
        assertEquals(dis.readLong(), 4L);
        assertEquals(dis.readLong(), 1240L);

        // Should be at EOF
        assertEquals(dis.available(), 0);

        index.close();
        restored.close();
    }

    @Test
    public void testToStreamWithoutPerEntryIndex() throws Exception {
        // Build OLD format (no addEntryOffset calls)
        LedgerMetadata metadata = OffloadIndexTest.createLedgerMetadata(12345L);
        OffloadIndexBlockBuilder builder = OffloadIndexBlockBuilder.create()
            .withLedgerMetadata(metadata)
            .withDataBlockHeaderLength(128);
        builder.addBlock(0, 1, 1000);
        builder.addBlock(3, 2, 1000);

        OffloadIndexBlock index = builder.withDataObjectLength(2000).build();
        OffloadIndexBlock.IndexInputStream stream = index.toStream();
        byte[] serialized = stream.readAllBytes();

        // Old format: no directory magic at the end
        // Stream size should match old format exactly
        OffloadIndexBlock restored = (OffloadIndexBlock) OffloadIndexBlockBuilder.create()
            .fromStream(new ByteArrayInputStream(serialized));
        assertEquals(restored.getEntryCount(), 2);

        index.close();
        restored.close();
    }

    @Test
    public void testFromStreamBulkLoadsOffsetsCache() throws Exception {
        OffsetsCache cache = new OffsetsCache();
        long ledgerId = 12345L;

        // Build index with per-entry offsets
        LedgerMetadata metadata = OffloadIndexTest.createLedgerMetadata(ledgerId);
        OffloadIndexBlockBuilder builder = OffloadIndexBlockBuilder.create()
            .withLedgerMetadata(metadata)
            .withDataBlockHeaderLength(128);

        builder.addBlock(0, 1, 1000);
        builder.addEntryOffset(0, 128);
        builder.addEntryOffset(1, 240);
        builder.addEntryOffset(2, 352);

        builder.addBlock(3, 2, 1000);
        builder.addEntryOffset(3, 1128);
        builder.addEntryOffset(4, 1240);

        OffloadIndexBlock index = builder.withDataObjectLength(2000).build();
        byte[] serialized = index.toStream().readAllBytes();
        index.close();

        // Deserialize with OffsetsCache loading
        OffloadIndexBlock restored = OffloadIndexBlockBuilder.create()
            .fromStream(new ByteArrayInputStream(serialized), ledgerId, cache);

        // Verify OffsetsCache was populated
        assertEquals(cache.getIfPresent(ledgerId, 0L), Long.valueOf(128L));
        assertEquals(cache.getIfPresent(ledgerId, 1L), Long.valueOf(240L));
        assertEquals(cache.getIfPresent(ledgerId, 2L), Long.valueOf(352L));
        assertEquals(cache.getIfPresent(ledgerId, 3L), Long.valueOf(1128L));
        assertEquals(cache.getIfPresent(ledgerId, 4L), Long.valueOf(1240L));

        // Block-level index should still work
        assertEquals(restored.getEntryCount(), 2);
        OffloadIndexEntry entry = restored.getIndexEntryForEntry(0);
        assertEquals(entry.getEntryId(), 0L);

        restored.close();
        cache.close();
    }

    @Test
    public void testFromStreamOldFormatNoPerEntryIndex() throws Exception {
        OffsetsCache cache = new OffsetsCache();
        long ledgerId = 12345L;

        // Build OLD format index (no addEntryOffset calls)
        LedgerMetadata metadata = OffloadIndexTest.createLedgerMetadata(ledgerId);
        OffloadIndexBlockBuilder builder = OffloadIndexBlockBuilder.create()
            .withLedgerMetadata(metadata)
            .withDataBlockHeaderLength(128);
        builder.addBlock(0, 1, 1000);
        builder.addBlock(3, 2, 1000);

        OffloadIndexBlock index = builder.withDataObjectLength(2000).build();
        byte[] serialized = index.toStream().readAllBytes();
        index.close();

        // Deserialize — should NOT crash, OffsetsCache should remain empty
        OffloadIndexBlock restored = OffloadIndexBlockBuilder.create()
            .fromStream(new ByteArrayInputStream(serialized), ledgerId, cache);

        assertNull(cache.getIfPresent(ledgerId, 0L));
        assertNull(cache.getIfPresent(ledgerId, 3L));
        assertEquals(restored.getEntryCount(), 2);

        restored.close();
        cache.close();
    }
}
