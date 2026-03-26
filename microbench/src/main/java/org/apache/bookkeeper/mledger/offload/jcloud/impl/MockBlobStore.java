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

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;

/**
 * A blob store backed by JClouds transient provider that injects configurable
 * latency on every {@code getBlob} call to simulate network round-trip time.
 */
public class MockBlobStore implements Closeable {

    private final BlobStoreContext context;
    private final BlobStore delegate;
    private final BlobStore proxy;
    private final String bucket;
    private final AtomicInteger requestCount = new AtomicInteger();
    private final long latencyMs;

    public MockBlobStore(String bucket, long latencyMs) {
        this.latencyMs = latencyMs;
        this.bucket = bucket;
        this.context = ContextBuilder.newBuilder("transient")
                .buildView(BlobStoreContext.class);
        this.delegate = context.getBlobStore();
        delegate.createContainerInLocation(null, bucket);

        this.proxy = (BlobStore) Proxy.newProxyInstance(
                BlobStore.class.getClassLoader(),
                new Class[]{BlobStore.class},
                (proxyObj, method, args) -> {
                    if ("getBlob".equals(method.getName())) {
                        if (latencyMs > 0) {
                            Thread.sleep(latencyMs);
                        }
                        requestCount.incrementAndGet();
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    public MockBlobStore(byte[] data, String bucket, String dataKey,
                         byte[] indexData, String indexKey, long latencyMs) {
        this(bucket, latencyMs);
        addLedger(data, dataKey, indexData, indexKey);
    }

    public void addLedger(byte[] data, String dataKey, byte[] indexData, String indexKey) {
        Blob dataBlob = delegate.blobBuilder(dataKey)
                .payload(data).contentLength(data.length).build();
        delegate.putBlob(bucket, dataBlob);

        Blob idxBlob = delegate.blobBuilder(indexKey)
                .payload(indexData).contentLength(indexData.length).build();
        delegate.putBlob(bucket, idxBlob);
    }

    public BlobStore getBlobStore() {
        return proxy;
    }

    public int getRequestCount() {
        return requestCount.get();
    }

    public void resetRequestCount() {
        requestCount.set(0);
    }

    @Override
    public void close() {
        context.close();
    }
}
