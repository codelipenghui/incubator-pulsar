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
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.TreeMap;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.net.BookieId;

/**
 * Generates a multi-block offloaded ledger data object and its sparse index
 * for use in benchmarks.
 *
 * <p>Data format per block:
 * <pre>
 *   [128B header: magic(4B) + headerLen(8B) + blockLen(8B) + firstEntryId(8B) + padding(100B)]
 *   [Entry 0: payloadLen(4B) + entryId(8B) + payload(N bytes)]
 *   [Entry 1: ...]
 *   ...
 * </pre>
 */
public class OffloadDataGenerator {

    public static final int HEADER_SIZE = 128;
    public static final int MAGIC_WORD = 0xFBDBABCB;
    public static final int ENTRY_HEADER_SIZE = 4 + 8;

    private final int numBlocks;
    private final int entriesPerBlock;
    private final int entryPayloadSize;
    private final int entrySize;
    private final int blockSize;
    private final int totalEntries;

    public OffloadDataGenerator(int numBlocks, int entriesPerBlock, int entryPayloadSize) {
        this.numBlocks = numBlocks;
        this.entriesPerBlock = entriesPerBlock;
        this.entryPayloadSize = entryPayloadSize;
        this.entrySize = ENTRY_HEADER_SIZE + entryPayloadSize;
        this.blockSize = HEADER_SIZE + entriesPerBlock * entrySize;
        this.totalEntries = numBlocks * entriesPerBlock;
    }

    public byte[] generateData() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(numBlocks * blockSize);
        DataOutputStream dos = new DataOutputStream(baos);
        byte[] payload = new byte[entryPayloadSize];

        for (int block = 0; block < numBlocks; block++) {
            long firstEntryId = (long) block * entriesPerBlock;

            // Block header (128 bytes)
            dos.writeInt(MAGIC_WORD);
            dos.writeLong(HEADER_SIZE);
            dos.writeLong(blockSize);
            dos.writeLong(firstEntryId);
            dos.write(new byte[HEADER_SIZE - 4 - 8 - 8 - 8]); // 100B padding

            // Entries
            for (int i = 0; i < entriesPerBlock; i++) {
                dos.writeInt(entryPayloadSize);    // payload length only
                dos.writeLong(firstEntryId + i);   // entry id
                dos.write(payload);
            }
        }

        dos.flush();
        return baos.toByteArray();
    }

    public TreeMap<Long, OffloadIndexEntryImpl> generateIndex() {
        TreeMap<Long, OffloadIndexEntryImpl> index = new TreeMap<>();
        for (int block = 0; block < numBlocks; block++) {
            long firstEntryId = (long) block * entriesPerBlock;
            long blockOffset = (long) block * blockSize;
            index.put(firstEntryId,
                    OffloadIndexEntryImpl.of(firstEntryId, 0, blockOffset, HEADER_SIZE));
        }
        return index;
    }

    /**
     * Returns the absolute byte offset where entry's 4B length field starts.
     */
    public long getEntryOffset(long entryId) {
        int block = (int) (entryId / entriesPerBlock);
        int entryInBlock = (int) (entryId % entriesPerBlock);
        long blockOffset = (long) block * blockSize;
        return blockOffset + HEADER_SIZE + (long) entryInBlock * entrySize;
    }

    public int getTotalEntries() {
        return totalEntries;
    }

    public long getDataLength() {
        return (long) numBlocks * blockSize;
    }

    public int getEntriesPerBlock() {
        return entriesPerBlock;
    }

    /**
     * Builds a real {@link OffloadIndexBlock} using the production builder,
     * serializes it to bytes suitable for storing in a blob store.
     */
    public byte[] generateSerializedIndex(long ledgerId) throws IOException {
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
                .withId(ledgerId)
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withDigestType(DigestType.CRC32C)
                .withPassword("pwd".getBytes(UTF_8))
                .withClosedState()
                .withLastEntryId(totalEntries - 1)
                .withLength(getDataLength())
                .newEnsembleEntry(0L, Arrays.asList(BookieId.parse("127.0.0.1:3181")))
                .build();

        OffloadIndexBlockBuilder builder = OffloadIndexBlockBuilder.create();
        builder.withLedgerMetadata(metadata)
               .withDataBlockHeaderLength(HEADER_SIZE);
        for (int block = 0; block < numBlocks; block++) {
            long firstEntryId = (long) block * entriesPerBlock;
            builder.addBlock(firstEntryId, block + 1, blockSize);
            // Add per-entry offsets for fine-grained index
            for (int i = 0; i < entriesPerBlock; i++) {
                long entryId = firstEntryId + i;
                builder.addEntryOffset(entryId, getEntryOffset(entryId));
            }
        }
        builder.withDataObjectLength(getDataLength());
        OffloadIndexBlock indexBlock = builder.build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OffloadIndexBlock.IndexInputStream stream = indexBlock.toStream();
        stream.transferTo(baos);
        stream.close();
        indexBlock.close();
        return baos.toByteArray();
    }
}
