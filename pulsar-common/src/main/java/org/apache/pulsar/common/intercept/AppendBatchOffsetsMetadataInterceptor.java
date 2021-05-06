/**
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
package org.apache.pulsar.common.intercept;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;

/**
 * A plugin for support broker level message batch.
 * The interceptor will add the batch offset into the broker entry metadata, so that the client can split
 * the individual batch by the batch offset.
 */
public class AppendBatchOffsetsMetadataInterceptor implements BrokerEntryMetadataInterceptor {

    @Override
    public BrokerEntryMetadata intercept(BrokerEntryMetadata brokerMetadata, ByteBuf headersAndPayload) {
        if (headersAndPayload instanceof CompositeByteBuf) {
            CompositeByteBuf cbb = (CompositeByteBuf) headersAndPayload;
            long offset = 0;
            for (ByteBuf byteBuf : cbb) {
                brokerMetadata.addBatchOffset(offset);
                offset += byteBuf.readableBytes();
            }
        }
        return brokerMetadata;
    }

    @Override
    public BrokerEntryMetadata interceptWithNumberOfMessages(BrokerEntryMetadata brokerMetadata, int numberOfMessages) {
        return brokerMetadata;
    }
}
