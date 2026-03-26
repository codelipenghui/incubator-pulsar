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

import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockV2;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockV2Builder;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for builder of index block used for offload a ledger to long term storage.
 */
public class OffloadIndexBlockV2BuilderImpl implements OffloadIndexBlockBuilder, OffloadIndexBlockV2Builder {
    private static final Logger log = LoggerFactory.getLogger(OffloadIndexBlockV2BuilderImpl.class);

    private final Map<Long, LedgerInfo> ledgerMetadataMap;
    private LedgerMetadata ledgerMetadata;
    private long dataObjectLength;
    private long dataHeaderLength;
    private List<OffloadIndexEntryImpl> entries;
    private int lastBlockSize;
    private int lastStreamingBlockSize;
    private long streamingOffset = 0;
    private final SortedMap<Long, List<OffloadIndexEntryImpl>> entryMap = new TreeMap<>();

    // Per-entry offset tracking: maps blockFirstEntryId -> list of [entryId, offset] pairs
    private final TreeMap<Long, List<long[]>> perEntryOffsets = new TreeMap<>();
    private long currentBlockFirstEntryId = -1;


    public OffloadIndexBlockV2BuilderImpl() {
        this.entries = Lists.newArrayList();
        this.ledgerMetadataMap = new HashMap<>();
    }

    @Override
    public OffloadIndexBlockV2BuilderImpl withDataObjectLength(long dataObjectLength) {
        this.dataObjectLength = dataObjectLength;
        return this;
    }

    @Override
    public OffloadIndexBlockV2BuilderImpl withDataBlockHeaderLength(long dataHeaderLength) {
        this.dataHeaderLength = dataHeaderLength;
        return this;
    }

    @Override
    public OffloadIndexBlockV2BuilderImpl withLedgerMetadata(LedgerMetadata metadata) {
        this.ledgerMetadata = metadata;
        return this;
    }

    @Override
    public OffloadIndexBlockV2BuilderImpl addLedgerMeta(Long ledgerId, LedgerInfo metadata) {
        this.ledgerMetadataMap.put(ledgerId, metadata);
        return this;
    }

    @Override
    public OffloadIndexBlockBuilder addBlock(long firstEntryId, int partId, int blockSize) {
        checkState(dataHeaderLength > 0);

        // we should added one by one.
        long offset;
        if (firstEntryId == 0) {
            checkState(entries.size() == 0);
            offset = 0;
        } else {
            checkState(entries.size() > 0);
            offset = entries.get(entries.size() - 1).getOffset() + lastBlockSize;
        }
        lastBlockSize = blockSize;

        this.entries.add(OffloadIndexEntryImpl.of(firstEntryId, partId, offset, dataHeaderLength));
        this.currentBlockFirstEntryId = firstEntryId;
        return this;
    }

    @Override
    public OffloadIndexBlockBuilder addEntryOffset(long entryId, long offset) {
        checkState(currentBlockFirstEntryId >= 0, "addBlock must be called before addEntryOffset");
        perEntryOffsets.computeIfAbsent(currentBlockFirstEntryId, k -> new ArrayList<>())
                .add(new long[]{entryId, offset});
        return this;
    }

    @Override
    public OffloadIndexBlockV2Builder addBlock(long ledgerId, long firstEntryId, int partId, int blockSize) {
        checkState(dataHeaderLength > 0);

        streamingOffset = streamingOffset + lastStreamingBlockSize;
        lastStreamingBlockSize = blockSize;

        final List<OffloadIndexEntryImpl> list = entryMap.getOrDefault(ledgerId, new LinkedList<>());
        list.add(OffloadIndexEntryImpl.of(firstEntryId, partId, streamingOffset, dataHeaderLength));
        entryMap.put(ledgerId, list);
        return this;
    }

    @Override
    public OffloadIndexBlockV2 fromStream(InputStream is) throws IOException {
        final DataInputStream dataInputStream = new DataInputStream(is);
        final int magic = dataInputStream.readInt();
        if (magic == OffloadIndexBlockImpl.getIndexMagicWord()) {
            return OffloadIndexBlockImpl.get(magic, dataInputStream);
        } else if (magic == OffloadIndexBlockV2Impl.getIndexMagicWord()) {
            return OffloadIndexBlockV2Impl.get(magic, dataInputStream);
        } else {
            throw new IOException(String.format("Invalid MagicWord. read: 0x%x  expected: 0x%x or 0x%x",
                    magic, OffloadIndexBlockImpl.getIndexMagicWord(),
                    OffloadIndexBlockV2Impl.getIndexMagicWord()));
        }
    }

    @Override
    public OffloadIndexBlock fromStream(InputStream is, long ledgerId, OffsetsCache offsetsCache) throws IOException {
        // Read all bytes upfront so that available() works reliably on ByteArrayInputStream
        byte[] allBytes = is.readAllBytes();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(allBytes));

        // Read magic and dispatch to block-level parser
        int magic = dis.readInt();
        OffloadIndexBlock block;
        if (magic == OffloadIndexBlockImpl.getIndexMagicWord()) {
            block = (OffloadIndexBlock) OffloadIndexBlockImpl.get(magic, dis);
        } else {
            throw new IOException(String.format("Invalid MagicWord. read: 0x%x  expected: 0x%x",
                    magic, OffloadIndexBlockImpl.getIndexMagicWord()));
        }

        // After block-level parsing, check if there is per-entry index data remaining
        if (dis.available() >= 4) {
            int directoryMagic = dis.readInt();
            if (directoryMagic == OffloadIndexBlockImpl.getEntryIndexDirectoryMagic()) {
                parsePerEntryIndex(dis, allBytes.length, ledgerId, offsetsCache);
            } else {
                log.warn("Unexpected data after block entries: magic=0x{}, ignoring",
                        Integer.toHexString(directoryMagic));
            }
        }

        return block;
    }

    /**
     * Parse the per-entry directory and sections from the DataInputStream,
     * and bulk-load all entry offsets into the OffsetsCache.
     *
     * @param dis the DataInputStream positioned right after the directory magic
     * @param totalStreamLength total length of the underlying byte array
     * @param ledgerId the ledger ID for the cache
     * @param offsetsCache the cache to populate
     */
    private void parsePerEntryIndex(DataInputStream dis, int totalStreamLength,
                                    long ledgerId, OffsetsCache offsetsCache) throws IOException {
        if (dis.available() < 20) {
            return;
        }

        // Read the first directory entry to determine how many directory entries exist.
        // Each directory entry is: firstEntryId(8B) + sectionOffset(8B) + sectionLength(4B) = 20B.
        // The first entry's sectionOffset tells us the absolute byte position where sections start.
        long firstDirEntryId = dis.readLong();
        long firstSectionOffset = dis.readLong();
        int firstSectionLength = dis.readInt();

        // Current position in the byte array after reading the first directory entry
        int currentPos = totalStreamLength - dis.available();
        // The remaining directory entries occupy the bytes between currentPos and firstSectionOffset
        int remainingDirBytes = (int) (firstSectionOffset - currentPos);
        int remainingDirEntries = remainingDirBytes / 20;

        int totalDirEntries = 1 + remainingDirEntries;

        // Skip the remaining directory entries — we don't need them for sequential reading
        for (int i = 0; i < remainingDirEntries; i++) {
            dis.readLong();  // firstEntryId
            dis.readLong();  // sectionOffset
            dis.readInt();   // sectionLength
        }

        // Read all per-entry sections sequentially
        Map<Long, Long> allOffsets = new HashMap<>();
        for (int d = 0; d < totalDirEntries; d++) {
            int entryCount = dis.readInt();
            for (int i = 0; i < entryCount; i++) {
                long entryId = dis.readLong();
                long offset = dis.readLong();
                allOffsets.put(entryId, offset);
            }
        }

        // Bulk-load all offsets into the cache
        if (!allOffsets.isEmpty()) {
            offsetsCache.bulkPut(ledgerId, allOffsets);
        }
    }

    @Override
    public OffloadIndexBlock build() {
        checkState(ledgerMetadata != null);
        checkState(!entries.isEmpty());
        checkState(dataObjectLength > 0);
        checkState(dataHeaderLength > 0);
        OffloadIndexBlockImpl block = OffloadIndexBlockImpl.get(
                ledgerMetadata, dataObjectLength, dataHeaderLength, entries);
        if (!perEntryOffsets.isEmpty()) {
            block.setPerEntryOffsets(perEntryOffsets);
        }
        return block;
    }

    @Override
    public OffloadIndexBlockV2 buildV2() {
        checkState(!ledgerMetadataMap.isEmpty());
        checkState(true);
        checkState(!entryMap.isEmpty());
        checkState(dataObjectLength > 0);
        checkState(dataHeaderLength > 0);
        return OffloadIndexBlockV2Impl.get(ledgerMetadataMap, dataObjectLength, dataHeaderLength, entryMap);
    }

}
