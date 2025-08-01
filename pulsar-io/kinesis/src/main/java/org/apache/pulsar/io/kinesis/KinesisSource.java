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
package org.apache.pulsar.io.kinesis;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.aws.AbstractAwsConnector;
import org.apache.pulsar.io.aws.AwsCredentialProviderPlugin;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

/**
 * @see ConfigsBuilder
 */
@Connector(
        name = "kinesis",
        type = IOType.SOURCE,
        help = "A source connector that copies messages from Kinesis to Pulsar",
        configClass = KinesisSourceConfig.class
    )
@Slf4j
public class KinesisSource extends AbstractAwsConnector implements Source<byte[]> {
    private LinkedBlockingQueue<KinesisRecord> queue;
    private KinesisSourceConfig kinesisSourceConfig;
    private ConfigsBuilder configsBuilder;
    private ShardRecordProcessorFactory recordProcessorFactory;
    private String workerId;
    private Scheduler scheduler;
    private Thread schedulerThread;
    private Throwable threadEx;
    private ScheduledExecutorService checkpointExecutor;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        this.kinesisSourceConfig = KinesisSourceConfig.load(config, sourceContext);
        queue = new LinkedBlockingQueue<>(kinesisSourceConfig.getReceiveQueueSize());
        workerId = String.valueOf(sourceContext.getInstanceId());

        this.checkpointExecutor = Executors.newSingleThreadScheduledExecutor();

        AwsCredentialProviderPlugin credentialsProvider = createCredentialProvider(
                kinesisSourceConfig.getAwsCredentialPluginName(),
                kinesisSourceConfig.getAwsCredentialPluginParam());

        KinesisAsyncClient kClient = kinesisSourceConfig.buildKinesisAsyncClient(credentialsProvider);
        recordProcessorFactory = new KinesisRecordProcessorFactory(queue, kinesisSourceConfig,
                sourceContext, checkpointExecutor);
        configsBuilder = new ConfigsBuilder(kinesisSourceConfig.getAwsKinesisStreamName(),
                                            kinesisSourceConfig.getApplicationName(),
                                            kClient,
                                            kinesisSourceConfig.buildDynamoAsyncClient(credentialsProvider),
                                            kinesisSourceConfig.buildCloudwatchAsyncClient(credentialsProvider),
                                            workerId,
                                            recordProcessorFactory);

        RetrievalConfig retrievalConfig = configsBuilder.retrievalConfig();
        if (!kinesisSourceConfig.isUseEnhancedFanOut()) {
            retrievalConfig.retrievalSpecificConfig(
                    new PollingConfig(kinesisSourceConfig.getAwsKinesisStreamName(),
                                      kClient));
        }

        retrievalConfig.initialPositionInStreamExtended(kinesisSourceConfig.getStreamStartPosition());

        scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                retrievalConfig
        );
        schedulerThread = new Thread(scheduler);
        schedulerThread.setDaemon(true);
        threadEx = null;
        schedulerThread.setUncaughtExceptionHandler((t, ex) -> {
            threadEx = ex;
        });
        schedulerThread.start();
    }

    @Override
    public KinesisRecord read() throws Exception {
        try {
            return queue.take();
        } catch (InterruptedException ex) {
            log.warn("Got interrupted when trying to fetch out of the queue");
            if (threadEx != null) {
                log.error("error from scheduler", threadEx);
            }
            throw ex;
        }
    }

    @Override
    public void close() throws Exception {
        if (scheduler != null) {
            scheduler.shutdown();
        }
        if (schedulerThread != null) {
            schedulerThread.join(30000L);
        }
        if (checkpointExecutor != null) {
            checkpointExecutor.shutdownNow();
            checkpointExecutor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
}