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
package org.apache.pulsar.tests.integration.functions;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.common.functions.BatchingConfig;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.MessagePayloadProcessorConfig;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.policies.data.FunctionStatusUtil;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.api.examples.AutoSchemaFunction;
import org.apache.pulsar.functions.api.examples.AvroSchemaTestFunction;
import org.apache.pulsar.functions.api.examples.InitializableFunction;
import org.apache.pulsar.functions.api.examples.MergeTopicFunction;
import org.apache.pulsar.functions.api.examples.RecordFunction;
import org.apache.pulsar.functions.api.examples.pojo.AvroTestObject;
import org.apache.pulsar.functions.api.examples.pojo.Users;
import org.apache.pulsar.functions.api.examples.serde.CustomObject;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.Runtime;
import org.apache.pulsar.tests.integration.topologies.FunctionRuntimeType;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;

/**
 * A test base for testing functions.
 */
@Slf4j
public abstract class PulsarFunctionsTest extends PulsarFunctionsTestBase {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public PulsarFunctionsTest(FunctionRuntimeType functionRuntimeType) {
        super(functionRuntimeType);
    }

    protected Map<String, String> produceMessagesToInputTopic(String inputTopicName,
                                                              int numMessages) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(inputTopicName)
                .create();

        LinkedHashMap<String, String> kvs = new LinkedHashMap<>();
        for (int i = 0; i < numMessages; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            kvs.put(key, value);
            producer.newMessage()
                    .key(key)
                    .value(value)
                    .send();
        }
        return kvs;
    }

    protected void testFunctionLocalRun(Runtime runtime) throws Exception {
        if (functionRuntimeType == FunctionRuntimeType.THREAD) {
            return;
        }


        String inputTopicName =
                "persistent://public/default/test-function-local-run-" + runtime + "-input-" + randomName(8);
        String outputTopicName = "test-function-local-run-" + runtime + "-output-" + randomName(8);

        final int numMessages = 10;
        String cmd = "";
        CommandGenerator commandGenerator = new CommandGenerator();
        commandGenerator.setAdminUrl("pulsar://pulsar-broker-0:6650");
        commandGenerator.setSourceTopic(inputTopicName);
        commandGenerator.setSinkTopic(outputTopicName);
        commandGenerator.setFunctionName("localRunTest-" + randomName(8));
        commandGenerator.setRuntime(runtime);
        switch (runtime) {
            case JAVA:
                commandGenerator.setFunctionClassName(EXCLAMATION_JAVA_CLASS);
                cmd = commandGenerator.generateLocalRunCommand(null);
                break;
            case PYTHON:
                commandGenerator.setFunctionClassName(EXCLAMATION_PYTHON_CLASS);
                cmd = commandGenerator.generateLocalRunCommand(EXCLAMATION_PYTHON_FILE);
                break;
            case GO:
                commandGenerator.setFunctionClassName(null);
                cmd = commandGenerator.generateLocalRunCommand(EXCLAMATION_GO_FILE);
                break;
        }

        log.info("cmd: {}", cmd);
        pulsarCluster.getAnyWorker().execCmdAsync(cmd.split(" "));

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {

            admin.topics().createNonPartitionedTopic(inputTopicName);
            admin.topics().createNonPartitionedTopic(outputTopicName);
            retryStrategically((test) -> {
                try {
                    return admin.topics().getStats(inputTopicName).getSubscriptions().size() == 1;
                } catch (PulsarAdminException e) {
                    return false;
                }
            }, 30, 200);

            assertEquals(admin.topics().getStats(inputTopicName).getSubscriptions().size(), 1);

            // publish and consume result
            if (Runtime.JAVA == runtime || Runtime.PYTHON == runtime) {
                // java and python supports schema
                @Cleanup PulsarClient client = PulsarClient.builder()
                        .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                        .build();

                @Cleanup Consumer<String> consumer = client.newConsumer(Schema.STRING)
                        .topic(outputTopicName)
                        .subscriptionType(SubscriptionType.Exclusive)
                        .subscriptionName("test-sub")
                        .subscribe();

                @Cleanup Producer<String> producer = client.newProducer(Schema.STRING)
                        .topic(inputTopicName)
                        .create();

                for (int i = 0; i < numMessages; i++) {
                    producer.send("message-" + i);
                }

                Set<String> expectedMessages = new HashSet<>();
                for (int i = 0; i < numMessages; i++) {
                    expectedMessages.add("message-" + i + "!");
                }

                for (int i = 0; i < numMessages; i++) {
                    Message<String> msg = consumer.receive(60 * 2, TimeUnit.SECONDS);
                    log.info("Received: {}", msg.getValue());
                    assertTrue(expectedMessages.contains(msg.getValue()));
                    expectedMessages.remove(msg.getValue());
                }
                assertEquals(expectedMessages.size(), 0);

            } else {
                // golang doesn't support schema

                @Cleanup PulsarClient client = PulsarClient.builder()
                        .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                        .build();
                @Cleanup Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                        .topic(outputTopicName)
                        .subscriptionType(SubscriptionType.Exclusive)
                        .subscriptionName("test-sub")
                        .subscribe();

                @Cleanup Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                        .topic(inputTopicName)
                        .enableBatching(true)
                        .batcherBuilder(BatcherBuilder.DEFAULT)
                        .create();

                for (int i = 0; i < numMessages; i++) {
                    producer.newMessage().value(("message-" + i).getBytes(UTF_8)).send();
                }

                Set<String> expectedMessages = new HashSet<>();
                for (int i = 0; i < numMessages; i++) {
                    expectedMessages.add("message-" + i + "!");
                }

                for (int i = 0; i < numMessages; i++) {
                    Message<byte[]> msg = consumer.receive(60 * 2, TimeUnit.SECONDS);
                    String msgValue = new String(msg.getValue(), UTF_8);
                    log.info("Received: {}", msgValue);
                    assertTrue(expectedMessages.contains(msgValue));
                    expectedMessages.remove(msgValue);
                }
                assertEquals(expectedMessages.size(), 0);
            }
        }

    }

    protected void testWindowFunction(String type, String[] expectedResults) throws Exception {
        int numOfMessages = 100;
        int windowLengthCount = 10;
        int slidingIntervalCount = 5;
        String functionName = "test-" + type + "-window-fn-" + randomName(8);

        String inputTopicName = "test-" + type + "-count-window-" + functionRuntimeType + "-input-" + randomName(8);
        String outputTopicName = "test-" + type + "-count-window-" + functionRuntimeType + "-output-" + randomName(8);
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {
            admin.topics().createNonPartitionedTopic(inputTopicName);
            admin.topics().createNonPartitionedTopic(outputTopicName);
        }

        CommandGenerator generator = CommandGenerator.createDefaultGenerator(
                inputTopicName,
                "org.apache.pulsar.functions.api.examples.WindowDurationFunction");
        generator.setFunctionName(functionName);
        generator.setSinkTopic(outputTopicName);
        generator.setWindowLengthCount(windowLengthCount);
        if (type.equals("sliding")) {
            generator.setSlidingIntervalCount(slidingIntervalCount);
        }


        String[] commands = {
                "sh", "-c", generator.generateCreateFunctionCommand()
        };

        ContainerExecResult containerExecResult = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(containerExecResult.getStdout().contains("Created successfully"));

        // get function info
        getFunctionInfoSuccess(functionName);

        containerExecResult = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "status",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName
        );

        FunctionStatus functionStatus = FunctionStatusUtil.decode(containerExecResult.getStdout());
        assertEquals(functionStatus.getNumInstances(), 1);
        assertEquals(functionStatus.getNumRunning(), 1);
        assertEquals(functionStatus.getInstances().size(), 1);
        assertEquals(functionStatus.getInstances().get(0).getInstanceId(), 0);
        assertEquals(functionStatus.getInstances().get(0).getStatus().isRunning(), true);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getNumReceived(), 0);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getNumSuccessfullyProcessed(), 0);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getLatestUserExceptions().size(), 0);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getLatestSystemExceptions().size(), 0);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup
        Reader<byte[]> reader = client.newReader().startMessageId(MessageId.earliest)
                .topic(outputTopicName)
                .create();

        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic(inputTopicName)
                .enableBatching(false)
                .create();

        // send 3 messages first, and it won't trigger the window and so these 3 messages will not be acked
        for (int i = 0; i < 3; i++) {
            producer.send(String.format("%d", i).getBytes());
        }
        TopicStats stats = pulsarAdmin.topics().getStats(inputTopicName, true);
        SubscriptionStats subStats = stats.getSubscriptions().get("public/default/" + functionName);
        assertNotNull(subStats);
        assertEquals(3, subStats.getMsgBacklog());
        assertEquals(3, subStats.getUnackedMessages());

        for (int i = 3; i < numOfMessages; i++) {
            producer.send(String.format("%d", i).getBytes());
        }

        int i = 0;
        while (true) {
            if (i > expectedResults.length) {
                Assertions.fail("More results than expected");
            }

            Message<byte[]> msg = reader.readNext(30, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            String msgStr = new String(msg.getData());
            log.info("[testWindowFunction] i: {} RECV: {}", i, msgStr);
            String result = msgStr.split(":")[0];
            assertThat(result).contains(expectedResults[i]);
            i++;
        }

        getFunctionStatus(functionName, numOfMessages, true);

        // in case last commit is not updated
        assertThat(i).isGreaterThanOrEqualTo(expectedResults.length - 1);

        // test that all messages are acked
        stats = pulsarAdmin.topics().getStats(inputTopicName, true);
        subStats = stats.getSubscriptions().get("public/default/" + functionName);
        assertNotNull(subStats);
        assertEquals(0, subStats.getMsgBacklog());
        assertEquals(0, subStats.getUnackedMessages());

        deleteFunction(functionName);

        getFunctionInfoNotFound(functionName);
    }

    protected void testFunctionNegAck(Runtime runtime) throws Exception {
        if (functionRuntimeType == FunctionRuntimeType.THREAD) {
            return;
        }


        Schema<?> schema;
        if (Runtime.JAVA == runtime || Runtime.PYTHON == runtime) {
            schema = Schema.STRING;
        } else {
            schema = Schema.BYTES;
        }

        String inputTopicName = "persistent://public/default/test-neg-ack-" + runtime + "-input-" + randomName(8);
        String outputTopicName = "test-neg-ack-" + runtime + "-output-" + randomName(8);
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {
            admin.topics().createNonPartitionedTopic(inputTopicName);
            admin.topics().createNonPartitionedTopic(outputTopicName);
        }

        String functionName = "test-neg-ack-fn-" + randomName(8);
        final int numMessages = 20;

        // submit the exclamation function

        if (runtime == Runtime.PYTHON) {
            submitFunction(
                    runtime, inputTopicName, outputTopicName, functionName, EXCEPTION_FUNCTION_PYTHON_FILE,
                    EXCEPTION_PYTHON_CLASS, schema, null);
        } else {
            submitFunction(
                    runtime, inputTopicName, outputTopicName, functionName, null, EXCEPTION_JAVA_CLASS, schema, null);
        }

        // get function info
        getFunctionInfoSuccess(functionName);

        // get function stats
        getFunctionStatsEmpty(functionName);

        // publish and consume result
        if (Runtime.JAVA == runtime || Runtime.PYTHON == runtime) {
            // java and python supports schema
            @Cleanup PulsarClient client = PulsarClient.builder()
                    .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                    .build();
            @Cleanup Consumer<String> consumer = client.newConsumer(Schema.STRING)
                    .topic(outputTopicName)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionName("test-sub")
                    .subscribe();
            @Cleanup Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(inputTopicName)
                    .create();

            for (int i = 0; i < numMessages; i++) {
                producer.send("message-" + i);
            }

            Set<String> expectedMessages = new HashSet<>();
            for (int i = 0; i < numMessages; i++) {
                expectedMessages.add("message-" + i + "!");
            }

            for (int i = 0; i < numMessages; i++) {
                Message<String> msg = consumer.receive(60 * 2, TimeUnit.SECONDS);
                log.info("Received: {}", msg.getValue());
                assertTrue(expectedMessages.contains(msg.getValue()));
                expectedMessages.remove(msg.getValue());
            }
            assertEquals(expectedMessages.size(), 0);

        } else {
            // golang doesn't support schema

            @Cleanup PulsarClient client = PulsarClient.builder()
                    .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                    .build();

            @Cleanup Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                    .topic(outputTopicName)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionName("test-sub")
                    .subscribe();

            @Cleanup Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                    .topic(inputTopicName)
                    .create();

            for (int i = 0; i < numMessages; i++) {
                producer.newMessage().value(("message-" + i).getBytes(UTF_8)).send();
            }

            Set<String> expectedMessages = new HashSet<>();
            for (int i = 0; i < numMessages; i++) {
                expectedMessages.add("message-" + i + "!");
            }

            for (int i = 0; i < numMessages; i++) {
                Message<byte[]> msg = consumer.receive(60 * 2, TimeUnit.SECONDS);
                String msgValue = new String(msg.getValue(), UTF_8);
                log.info("Received: {}", msgValue);
                assertTrue(expectedMessages.contains(msgValue));
                expectedMessages.remove(msgValue);
            }
            assertEquals(expectedMessages.size(), 0);
        }

        // get function status
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "status",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName
        );

        FunctionStatus functionStatus = FunctionStatusUtil.decode(result.getStdout());

        assertEquals(functionStatus.getNumInstances(), 1);
        assertEquals(functionStatus.getNumRunning(), 1);
        assertEquals(functionStatus.getInstances().size(), 1);
        assertEquals(functionStatus.getInstances().get(0).getInstanceId(), 0);
        assertTrue(functionStatus.getInstances().get(0).getStatus().getAverageLatency() > 0.0);
        assertEquals(functionStatus.getInstances().get(0).getStatus().isRunning(), true);
        assertTrue(functionStatus.getInstances().get(0).getStatus().getLastInvocationTime() > 0);
        // going to receive two more tuples because of delivery
        assertEquals(functionStatus.getInstances().get(0).getStatus().getNumReceived(), numMessages + 2);
        // only going to successfully process 20
        assertEquals(functionStatus.getInstances().get(0).getStatus().getNumSuccessfullyProcessed(), numMessages);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getNumRestarts(), 0);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getLatestUserExceptions().size(), 2);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getLatestSystemExceptions().size(), 0);

        // get function stats
        result = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "stats",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName
        );

        log.info("FUNCTION STATS: {}", result.getStdout());

        FunctionStatsImpl functionStats = FunctionStatsImpl.decode(result.getStdout());
        assertEquals(functionStats.getReceivedTotal(), numMessages + 2);
        assertEquals(functionStats.getProcessedSuccessfullyTotal(), numMessages);
        assertEquals(functionStats.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.getUserExceptionsTotal(), 2);
        assertTrue(functionStats.avgProcessLatency > 0);
        assertTrue(functionStats.getLastInvocation() > 0);

        assertEquals(functionStats.instances.size(), 1);
        assertEquals(functionStats.instances.get(0).getInstanceId(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getReceivedTotal(), numMessages + 2);
        assertEquals(functionStats.instances.get(0).getMetrics().getProcessedSuccessfullyTotal(), numMessages);
        assertEquals(functionStats.instances.get(0).getMetrics().getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getUserExceptionsTotal(), 2);
        assertTrue(functionStats.instances.get(0).getMetrics().getAvgProcessLatency() > 0);

        // delete function
        deleteFunction(functionName);

        // get function info
        getFunctionInfoNotFound(functionName);

        // make sure subscriptions are cleanup
        checkSubscriptionsCleanup(inputTopicName);
    }

    public void testGoPublishFunction() throws Exception {
        testPublishFunction(Runtime.GO);
    }

    protected void testPublishFunction(Runtime runtime) throws Exception {
        if (functionRuntimeType == FunctionRuntimeType.THREAD) {
            return;
        }

        Schema<?> schema;
        if (Runtime.JAVA == runtime || Runtime.PYTHON == runtime) {
            schema = Schema.STRING;
        } else {
            schema = Schema.BYTES;
        }


        String inputTopicName = "persistent://public/default/test-publish-" + runtime + "-input-" + randomName(8);
        String outputTopicName = "test-publish-" + runtime + "-output-" + randomName(8);
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {
            admin.topics().createNonPartitionedTopic(inputTopicName);
            admin.topics().createNonPartitionedTopic(outputTopicName);
        }

        String functionName = "test-publish-fn-" + randomName(8);
        final int numMessages = 10;

        // submit the exclamation function
        switch (runtime) {
            case JAVA:
                submitFunction(
                        runtime,
                        inputTopicName,
                        outputTopicName,
                        functionName,
                        null,
                        PUBLISH_JAVA_CLASS,
                        schema,
                        Collections.singletonMap("publish-topic", outputTopicName),
                        null, null, null, null, null, null);
                break;
            case PYTHON:
                ConsumerConfig consumerConfig = new ConsumerConfig();
                consumerConfig.setSchemaType("string");
                Map<String, String> inputSpecs = new HashMap<>() {{
                    put(inputTopicName, objectMapper.writeValueAsString(consumerConfig));
                }};
                submitFunction(
                        runtime,
                        inputTopicName,
                        outputTopicName,
                        functionName,
                        PUBLISH_FUNCTION_PYTHON_FILE,
                        PUBLISH_PYTHON_CLASS,
                        schema,
                        Collections.singletonMap("publish-topic", outputTopicName),
                        objectMapper.writeValueAsString(inputSpecs), "string", null, null, null, null);
                break;
            case GO:
                submitFunction(
                        runtime,
                        inputTopicName,
                        outputTopicName,
                        functionName,
                        PUBLISH_FUNCTION_GO_FILE,
                        null,
                        schema,
                        Collections.singletonMap("publish-topic", outputTopicName),
                        null, null, null, null, null, null);
        }

        // get function info
        getFunctionInfoSuccess(functionName);

        // get function stats
        getFunctionStatsEmpty(functionName);

        // publish and consume result

        if (Runtime.JAVA == runtime || Runtime.PYTHON == runtime) {
            // java and python supports schema
            publishAndConsumeMessages(inputTopicName, outputTopicName, numMessages);
        } else {
            // Does Go support schema? Maybe we need a switch instead for the Go case.

            @Cleanup PulsarClient client = PulsarClient.builder()
                    .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                    .build();

            @Cleanup Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                    .topic(outputTopicName)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionName("test-sub")
                    .subscribe();

            @Cleanup Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                    .topic(inputTopicName)
                    .create();

            for (int i = 0; i < numMessages; i++) {
                producer.newMessage().key(String.valueOf(i)).property("count", String.valueOf(i))
                        .value(("message-" + i).getBytes(UTF_8)).send();
            }

            Set<String> expectedMessages = new HashSet<>();
            for (int i = 0; i < numMessages; i++) {
                expectedMessages.add("message-" + i + "!");
            }

            for (int i = 0; i < numMessages; i++) {
                Message<byte[]> msg = consumer.receive(30, TimeUnit.SECONDS);
                String msgValue = new String(msg.getValue(), UTF_8);
                log.info("Received: {}", msgValue);
                assertEquals(msg.getKey(), String.valueOf(i));
                assertEquals(msg.getProperties().get("count"), String.valueOf(i));
                assertEquals(msg.getProperties().get("input_topic"), inputTopicName);
                assertTrue(msg.getEventTime() > 0);
                assertTrue(expectedMessages.contains(msgValue));
                expectedMessages.remove(msgValue);
            }
        }

        // get function status
        getFunctionStatus(functionName, numMessages, true);

        // get function stats
        getFunctionStats(functionName, numMessages);

        // delete function
        deleteFunction(functionName);

        // get function info
        getFunctionInfoNotFound(functionName);

        // make sure subscriptions are cleanup
        checkSubscriptionsCleanup(inputTopicName);
    }

    protected void testExclamationFunction(Runtime runtime,
                                           boolean isTopicPattern,
                                           boolean pyZip,
                                           boolean multipleInput,
                                           boolean withExtraDeps) throws Exception {
        testExclamationFunction(runtime, isTopicPattern, pyZip, multipleInput, withExtraDeps, null, null, null);
    }

    protected void testExclamationFunction(Runtime runtime,
                                           boolean isTopicPattern,
                                           boolean pyZip,
                                           boolean multipleInput,
                                           boolean withExtraDeps,
                                           ConsumerConfig consumerConfig) throws Exception {
        testExclamationFunction(runtime, isTopicPattern, pyZip, multipleInput, withExtraDeps, null, null, null);
    }

    protected void testExclamationFunction(Runtime runtime,
                                           boolean isTopicPattern,
                                           boolean pyZip,
                                           boolean multipleInput,
                                           boolean withExtraDeps,
                                           ProducerConfig producerConfig) throws Exception {
        testExclamationFunction(runtime, isTopicPattern, pyZip, multipleInput, withExtraDeps, null,
                producerConfig, null);
    }

    protected void testExclamationFunction(Runtime runtime,
                                           boolean isTopicPattern,
                                           boolean pyZip,
                                           boolean multipleInput,
                                           boolean withExtraDeps,
                                           ConsumerConfig consumerConfig,
                                           ProducerConfig producerConfig,
                                           java.util.function.Consumer<CommandGenerator> commandGeneratorConsumer)
            throws Exception {
        if (functionRuntimeType == FunctionRuntimeType.THREAD && (runtime == Runtime.PYTHON || runtime == Runtime.GO)) {
            // python&go can only run on process mode
            return;
        }


        Schema<?> schema;
        if (Runtime.JAVA == runtime || Runtime.PYTHON == runtime) {
            schema = Schema.STRING;
        } else {
            schema = Schema.BYTES;
        }

        String inputTopicName = "persistent://public/default/test-exclamation-" + runtime + "-input-" + randomName(8);
        String outputTopicName = "test-exclamation-" + runtime + "-output-" + randomName(8);
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {
            admin.topics().createNonPartitionedTopic(inputTopicName);
            admin.topics().createNonPartitionedTopic(outputTopicName);
        }
        if (isTopicPattern) {
            inputTopicName = inputTopicName + ".*";
        } else if (multipleInput) {
            inputTopicName = inputTopicName + "1," + inputTopicName + "2";
        }
        String functionName = "test-exclamation-fn-" + randomName(8);
        final int numMessages = 10;

        // submit the exclamation function
        submitExclamationFunction(
                runtime, inputTopicName, outputTopicName, functionName, pyZip, withExtraDeps, schema,
                commandGeneratorConsumer);

        // get function info
        final String info = getFunctionInfoSuccess(functionName);
        FunctionConfig config = ObjectMapperFactory.getMapper().getObjectMapper().readValue(info, FunctionConfig.class);

        // get function stats
        getFunctionStatsEmpty(functionName);

        // publish and consume result
        if (Runtime.JAVA == runtime || Runtime.PYTHON == runtime) {
            // java supports schema
            publishAndConsumeMessages(inputTopicName, outputTopicName, numMessages);
        } else {
            // golang doesn't support schema
            publishAndConsumeMessagesBytes(inputTopicName, outputTopicName, numMessages);
        }

        // check batching config
        if (runtime == Runtime.JAVA) {
            BatchingConfig batchingConfig = null;
            if (producerConfig != null && producerConfig.getBatchingConfig() != null) {
                batchingConfig = producerConfig.getBatchingConfig();
            }
            checkLogs(functionName, batchingConfig, consumerConfig, config, inputTopicName);
        }

        // get function status
        getFunctionStatus(functionName, numMessages, true);

        if (Runtime.GO != runtime) {
            // TODO: Go runtime doesn't collect `process_latency_ms_1min` metric
            // get function stats
            getFunctionStats(functionName, numMessages);
        }

        // update parallelism
        updateFunctionParallelism(functionName, 2);

        //get function status
        getFunctionStatus(functionName, 0, true, 2);

        // update code file
        switch (runtime) {
            case JAVA:
                updateFunctionCodeFile(functionName, Runtime.JAVA, "test");
                break;
            case PYTHON:
                updateFunctionCodeFile(functionName, Runtime.PYTHON, EXCLAMATION_PYTHON_FILE);
                break;
            case GO:
                updateFunctionCodeFile(functionName, Runtime.GO, EXCLAMATION_GO_FILE);
                break;
        }

        // check subscription type
        checkSubscriptionType(inputTopicName, config);

        // delete function
        deleteFunction(functionName);

        // get function info
        getFunctionInfoNotFound(functionName);

        // make sure subscriptions are cleanup
        checkSubscriptionsCleanup(inputTopicName);

    }

    private void checkSubscriptionType(String topic, FunctionConfig config) {
        List<String> topics = new ArrayList<>();
        if (topic.endsWith(".*")) {
            topics.add(topic.substring(0, topic.length() - 2) + "1");
            topics.add(topic.substring(0, topic.length() - 2) + "2");
        } else if (topic.contains(",")) {
            topics.addAll(Arrays.asList(topic.split(",")));
        } else {
            topics.add(topic);
        }
        topics.stream().forEach(t -> {
            try {
                ContainerExecResult result = pulsarCluster.getAnyBroker().execCmd(
                        PulsarCluster.ADMIN_SCRIPT,
                        "topics",
                        "stats",
                        t);
                TopicStats topicStats = ObjectMapperFactory.getMapper().reader()
                        .readValue(result.getStdout(), TopicStats.class);
                assertEquals(topicStats.getSubscriptions().size(), 1);
                final SubscriptionStats sub = topicStats.getSubscriptions().values().iterator()
                        .next();
                if (config.getRetainOrdering()) {
                    assertEquals(sub.getType(), "Failover");
                } else if (config.getRetainKeyOrdering()) {
                    assertEquals(sub.getType(), "Key_Shared");
                } else {
                    assertEquals(sub.getType(), "Shared");
                }
            } catch (Exception e) {
                fail("Command should have exited with non-zero");
            }
        });
    }

    // checking batching config/consumer config, we can only check this by checking the logs for now
    private void checkLogs(String functionName, BatchingConfig config, ConsumerConfig consumerConfig,
                           FunctionConfig functionConfig, String topic) {
        if (config != null) {
            assertNotNull(functionConfig.getProducerConfig());
            assertNotNull(functionConfig.getProducerConfig().getBatchingConfig());
            assertEquals(config.toString(), functionConfig.getProducerConfig().getBatchingConfig().toString());
        }

        String functionLogs = pulsarCluster.getFunctionLogs(functionName);
        if (config == null || config.isEnabled()) {
            BatchingConfig finalConfig = config;
            if (finalConfig == null) {
                finalConfig = BatchingConfig.builder().build();
            }
            assertTrue(functionLogs.contains(finalConfig.toString()));

            // THREAD runtime doesn't include producer&consumer related logs in the function logs
            if (functionRuntimeType == FunctionRuntimeType.PROCESS) {
                if (finalConfig.getBatchingMaxMessages() == null) {
                    finalConfig.setBatchingMaxMessages(1000);
                }
                if (finalConfig.getBatchingMaxBytes() == null) {
                    finalConfig.setBatchingMaxBytes(128 * 1024);
                }
                if (finalConfig.getBatchingMaxPublishDelayMs() == null) {
                    finalConfig.setBatchingMaxPublishDelayMs(10);
                }
                if (finalConfig.getRoundRobinRouterBatchingPartitionSwitchFrequency() == null) {
                    finalConfig.setRoundRobinRouterBatchingPartitionSwitchFrequency(10);
                }
                String producerSpec = String.format(
                        "\"batchingMaxPublishDelayMicros\":%d,\"batchingPartitionSwitchFrequencyByPublishDelay\":%d,"
                                + "\"batchingMaxMessages\":%d,\"batchingMaxBytes\":%d,\"batchingEnabled\":%s",
                        finalConfig.getBatchingMaxPublishDelayMs() * 1000,
                        finalConfig.getRoundRobinRouterBatchingPartitionSwitchFrequency(),
                        finalConfig.getBatchingMaxMessages(),
                        finalConfig.getBatchingMaxBytes(), finalConfig.isEnabled());
                assertTrue(functionLogs.contains(producerSpec));
            }
        } else {
            assertTrue(functionLogs.contains("BatchingConfig(enabled=false"));
            // THREAD runtime doesn't include producer&consumer related logs in the function logs
            if (functionRuntimeType == FunctionRuntimeType.PROCESS) {
                assertTrue(functionLogs.contains("\"batchingEnabled\":false"));
            }
        }

        if (consumerConfig != null && consumerConfig.getMessagePayloadProcessorConfig() != null) {
            MessagePayloadProcessorConfig payloadProcessorConfig = consumerConfig.getMessagePayloadProcessorConfig();
            assertNotNull(functionConfig.getInputSpecs().get(topic));
            assertNotNull(functionConfig.getInputSpecs().get(topic).getMessagePayloadProcessorConfig());
            assertEquals(payloadProcessorConfig.toString(),
                    functionConfig.getInputSpecs().get(topic).getMessagePayloadProcessorConfig().toString());

            // THREAD runtime doesn't include producer&consumer related logs in the function logs
            if (functionRuntimeType == FunctionRuntimeType.PROCESS) {
                assertTrue(functionLogs.contains("Processing message using TestPayloadProcessor"));
            }
            if (payloadProcessorConfig.getConfig() == null || payloadProcessorConfig.getConfig().isEmpty()) {
                assertTrue(functionLogs.contains("TestPayloadProcessor constructor without configs"));
            } else {
                String configs = payloadProcessorConfig.getConfig().entrySet().stream()
                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                        .collect(Collectors.joining(", "));
                String expectedLogs = String.format("TestPayloadProcessor constructor with configs %s", configs);
                assertTrue(functionLogs.contains(expectedLogs));
            }
        }
    }

    private void submitExclamationFunction(Runtime runtime,
                                           String inputTopicName,
                                           String outputTopicName,
                                           String functionName,
                                           boolean pyZip,
                                           boolean withExtraDeps,
                                           Schema<?> schema) throws Exception {
        submitExclamationFunction(runtime, inputTopicName, outputTopicName, functionName, pyZip,
                withExtraDeps, schema, null);
    }

    private void submitExclamationFunction(Runtime runtime,
                                           String inputTopicName,
                                           String outputTopicName,
                                           String functionName,
                                           boolean pyZip,
                                           boolean withExtraDeps,
                                           Schema<?> schema,
                                           java.util.function.Consumer<CommandGenerator> commandGeneratorConsumer)
            throws Exception {
        submitFunction(
                runtime,
                inputTopicName,
                outputTopicName,
                functionName,
                pyZip,
                withExtraDeps,
                false,
                getExclamationClass(runtime, pyZip, withExtraDeps),
                schema,
                commandGeneratorConsumer);
    }

    private <T> void submitFunction(Runtime runtime,
                                    String inputTopicName,
                                    String outputTopicName,
                                    String functionName,
                                    boolean pyZip,
                                    boolean withExtraDeps,
                                    boolean isPublishFunction,
                                    String functionClass,
                                    Schema<T> inputTopicSchema,
                                    java.util.function.Consumer<CommandGenerator> commandGeneratorConsumer)
            throws Exception {

        String file = null;
        if (Runtime.JAVA == runtime) {
            file = null;
        } else if (Runtime.PYTHON == runtime) {
            if (isPublishFunction) {
                file = PUBLISH_FUNCTION_PYTHON_FILE;
            } else if (pyZip) {
                file = EXCLAMATION_PYTHON_ZIP_FILE;
            } else if (withExtraDeps) {
                file = EXCLAMATION_WITH_DEPS_PYTHON_FILE;
            } else {
                file = EXCLAMATION_PYTHON_FILE;
            }
        } else if (Runtime.GO == runtime) {
            if (isPublishFunction) {
                file = PUBLISH_FUNCTION_GO_FILE;
            } else {
                file = EXCLAMATION_GO_FILE;
            }
        }

        submitFunction(runtime, inputTopicName, outputTopicName, functionName, file, functionClass, inputTopicSchema,
                commandGeneratorConsumer);
    }

    private <T> void submitFunction(Runtime runtime,
                                    String inputTopicName,
                                    String outputTopicName,
                                    String functionName,
                                    String functionFile,
                                    String functionClass,
                                    Schema<T> inputTopicSchema,
                                    java.util.function.Consumer<CommandGenerator> commandGeneratorConsumer)
            throws Exception {
        submitFunction(runtime, inputTopicName, outputTopicName, functionName, functionFile, functionClass,
                inputTopicSchema, null, null, null, null, null, null,
                commandGeneratorConsumer);
    }

    private <T> void submitFunction(Runtime runtime,
                                    String inputTopicName,
                                    String outputTopicName,
                                    String functionName,
                                    String functionFile,
                                    String functionClass,
                                    Schema<T> inputTopicSchema,
                                    Map<String, String> userConfigs,
                                    String customSchemaInputs,
                                    String outputSchemaType,
                                    SubscriptionInitialPosition subscriptionInitialPosition,
                                    String inputTypeClassName,
                                    String outputTypeClassName,
                                    java.util.function.Consumer<CommandGenerator> commandGeneratorConsumer)
            throws Exception {

        if (StringUtils.isNotEmpty(inputTopicName)) {
            ensureSubscriptionCreated(
                    inputTopicName, String.format("public/default/%s", functionName), inputTopicSchema);
        }

        CommandGenerator generator;
        log.info("------- INPUT TOPIC: '{}', customSchemaInputs: {}", inputTopicName, customSchemaInputs);
        if (inputTopicName.endsWith(".*")) {
            log.info("----- CREATING TOPIC PATTERN FUNCTION --- ");
            generator = CommandGenerator.createTopicPatternGenerator(inputTopicName, functionClass);
        } else {
            log.info("----- CREATING REGULAR FUNCTION --- ");
            generator = CommandGenerator.createDefaultGenerator(inputTopicName, functionClass);
        }
        generator.setSinkTopic(outputTopicName);
        generator.setFunctionName(functionName);
        if (userConfigs != null) {
            generator.setUserConfig(userConfigs);
        }
        if (customSchemaInputs != null) {
            generator.setCustomSchemaInputs(customSchemaInputs);
        }
        if (outputSchemaType != null) {
            generator.setSchemaType(outputSchemaType);
        }
        if (subscriptionInitialPosition != null) {
            generator.setSubscriptionInitialPosition(subscriptionInitialPosition);
        }
        if (inputTypeClassName != null) {
            generator.setInputTypeClassName(inputTypeClassName);
        }
        if (outputTypeClassName != null) {
            generator.setOutputTypeClassName(outputTypeClassName);
        }
        if (commandGeneratorConsumer != null) {
            commandGeneratorConsumer.accept(generator);
        }
        String command = "";

        switch (runtime) {
            case JAVA:
                command = generator.generateCreateFunctionCommand();
                break;
            case PYTHON:
            case GO:
                generator.setRuntime(runtime);
                command = generator.generateCreateFunctionCommand(functionFile);
                break;
            default:
                throw new IllegalArgumentException("Unsupported runtime : " + runtime);
        }

        log.info("---------- Function command: {}", command);
        String[] commands = {
                "sh", "-c", command
        };
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                commands);
        log.info("---------- stdout is: {}", result.getStdout());
        log.info("---------- stderr is: {}", result.getStderr());
        assertTrue(result.getStdout().contains("Created successfully"));
    }

    private void updateFunctionParallelism(String functionName, int parallelism) throws Exception {

        CommandGenerator generator = new CommandGenerator();
        generator.setFunctionName(functionName);
        generator.setParallelism(parallelism);
        String command = generator.generateUpdateFunctionCommand();

        log.info("---------- Function command: {}", command);
        String[] commands = {
                "sh", "-c", command
        };
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                commands);
        assertTrue(result.getStdout().contains("Updated successfully"));
    }

    private void updateFunctionCodeFile(String functionName, Runtime runtime, String codeFile) throws Exception {

        CommandGenerator generator = new CommandGenerator();
        generator.setFunctionName(functionName);
        generator.setRuntime(runtime);
        String command = generator.generateUpdateFunctionCommand(codeFile);

        log.info("---------- Function command: {}", command);
        String[] commands = {
                "sh", "-c", command
        };
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                commands);
        assertTrue(result.getStdout().contains("Updated successfully"));
    }

    protected <T> void submitFunction(Runtime runtime,
                                      String inputTopicName,
                                      String outputTopicName,
                                      String functionName,
                                      String functionFile,
                                      String functionClass,
                                      Map<String, String> inputSerdeClassNames,
                                      String outputSerdeClassName,
                                      Map<String, String> userConfigs) throws Exception {

        CommandGenerator generator;
        log.info("------- INPUT TOPIC: '{}'", inputTopicName);
        if (inputTopicName.endsWith(".*")) {
            log.info("----- CREATING TOPIC PATTERN FUNCTION --- ");
            generator = CommandGenerator.createTopicPatternGenerator(inputTopicName, functionClass);
        } else {
            log.info("----- CREATING REGULAR FUNCTION --- ");
            generator = CommandGenerator.createDefaultGenerator(inputTopicName, functionClass);
        }
        generator.setSinkTopic(outputTopicName);
        generator.setFunctionName(functionName);
        generator.setCustomSerDeSourceTopics(inputSerdeClassNames);
        generator.setOutputSerDe(outputSerdeClassName);
        if (userConfigs != null) {
            generator.setUserConfig(userConfigs);
        }
        String command;
        if (Runtime.JAVA == runtime) {
            command = generator.generateCreateFunctionCommand();
        } else if (Runtime.PYTHON == runtime) {
            generator.setRuntime(runtime);
            command = generator.generateCreateFunctionCommand(functionFile);
        } else {
            throw new IllegalArgumentException("Unsupported runtime : " + runtime);
        }

        log.info("---------- Function command: {}", command);
        String[] commands = {
                "sh", "-c", command
        };
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                commands);
        assertTrue(result.getStdout().contains("Created successfully"));
    }

    @SuppressWarnings("try")
    private <T> void ensureSubscriptionCreated(String inputTopicName,
                                               String subscriptionName,
                                               Schema<T> inputTopicSchema)
            throws Exception {
        // ensure the function subscription exists before we start producing messages
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build()) {
            List<String> topics = new ArrayList<>();
            if (inputTopicName.endsWith(".*")) {
                topics.add(inputTopicName.substring(0, inputTopicName.length() - 2) + "1");
                topics.add(inputTopicName.substring(0, inputTopicName.length() - 2) + "2");
            } else if (inputTopicName.contains(",")) {
                topics.addAll(Arrays.asList(inputTopicName.split(",")));
            } else {
                topics.add(inputTopicName);
            }
            try (Consumer<T> ignored = client.newConsumer(inputTopicSchema)
                    .topic(topics.toArray(new String[0]))
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName(subscriptionName)
                    .subscribe()) {
            }
        }
    }

    protected String getFunctionInfoSuccess(String functionName) throws Exception {
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "get",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName
        );

        log.info("FUNCTION STATE: {}", result.getStdout());
        assertTrue(result.getStdout().contains("\"name\": \"" + functionName + "\""));
        return result.getStdout();
    }


    protected void getFunctionStatsEmpty(String functionName) throws Exception {
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "stats",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName
        );

        log.info("FUNCTION STATS: {}", result.getStdout());
        FunctionStatsImpl functionStats = FunctionStatsImpl.decode(result.getStdout());

        assertEquals(functionStats.getReceivedTotal(), 0);
        assertEquals(functionStats.getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.getUserExceptionsTotal(), 0);
        assertEquals(functionStats.avgProcessLatency, null);
        assertEquals(functionStats.oneMin.getReceivedTotal(), 0);
        assertEquals(functionStats.oneMin.getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.oneMin.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.oneMin.getUserExceptionsTotal(), 0);
        assertEquals(functionStats.oneMin.getAvgProcessLatency(), null);
        assertEquals(functionStats.getAvgProcessLatency(), functionStats.oneMin.getAvgProcessLatency());
        assertEquals(functionStats.getLastInvocation(), null);

        assertEquals(functionStats.instances.size(), 1);
        assertEquals(functionStats.instances.get(0).getInstanceId(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getReceivedTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getUserExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getAvgProcessLatency(), null);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getReceivedTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getUserExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getAvgProcessLatency(), null);
    }

    private void getFunctionStats(String functionName, int numMessages) throws Exception {
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "stats",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName
        );

        log.info("FUNCTION STATS: {}", result.getStdout());

        FunctionStatsImpl functionStats = FunctionStatsImpl.decode(result.getStdout());
        assertEquals(functionStats.getReceivedTotal(), numMessages);
        assertEquals(functionStats.getProcessedSuccessfullyTotal(), numMessages);
        assertEquals(functionStats.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.getUserExceptionsTotal(), 0);
        assertTrue(functionStats.avgProcessLatency > 0);
        assertEquals(functionStats.oneMin.getReceivedTotal(), numMessages);
        assertEquals(functionStats.oneMin.getProcessedSuccessfullyTotal(), numMessages);
        assertEquals(functionStats.oneMin.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.oneMin.getUserExceptionsTotal(), 0);
        assertTrue(functionStats.oneMin.getAvgProcessLatency() > 0);
        assertEquals(functionStats.getAvgProcessLatency(), functionStats.oneMin.getAvgProcessLatency());
        assertTrue(functionStats.getLastInvocation() > 0);

        assertEquals(functionStats.instances.size(), 1);
        assertEquals(functionStats.instances.get(0).getInstanceId(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getReceivedTotal(), numMessages);
        assertEquals(functionStats.instances.get(0).getMetrics().getProcessedSuccessfullyTotal(), numMessages);
        assertEquals(functionStats.instances.get(0).getMetrics().getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getUserExceptionsTotal(), 0);
        assertTrue(functionStats.instances.get(0).getMetrics().getAvgProcessLatency() > 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getReceivedTotal(), numMessages);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getProcessedSuccessfullyTotal(),
                numMessages);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getUserExceptionsTotal(), 0);
        assertTrue(functionStats.instances.get(0).getMetrics().getOneMin().getAvgProcessLatency() > 0);
    }

    private void getFunctionInfoNotFound(String functionName) throws Exception {
        retryStrategically(aVoid -> {
            try {
                pulsarCluster.getAnyWorker().execCmd(
                        PulsarCluster.ADMIN_SCRIPT,
                        "functions",
                        "get",
                        "--tenant", "public",
                        "--namespace", "default",
                        "--name", functionName);
            } catch (ContainerExecException e) {
                if (e.getResult().getStderr().contains("Reason: Function " + functionName + " doesn't exist")) {
                    return true;
                }

            } catch (Exception e) {

            }
            return false;
        }, 5, 100, true);
    }

    private void checkSubscriptionsCleanup(String topic) throws Exception {
        List<String> topics = new ArrayList<>();
        if (topic.endsWith(".*")) {
            topics.add(topic.substring(0, topic.length() - 2) + "1");
            topics.add(topic.substring(0, topic.length() - 2) + "2");
        } else if (topic.contains(",")) {
            topics.addAll(Arrays.asList(topic.split(",")));
        } else {
            topics.add(topic);
        }
        topics.stream().forEach(t -> {
            try {
                ContainerExecResult result = pulsarCluster.getAnyBroker().execCmd(
                        PulsarCluster.ADMIN_SCRIPT,
                        "topics",
                        "stats",
                        t);
                TopicStats topicStats = ObjectMapperFactory.getMapper().reader()
                        .readValue(result.getStdout(), TopicStats.class);
                assertEquals(topicStats.getSubscriptions().size(), 0);
            } catch (Exception e) {
                fail("Command should have exited with non-zero");
            }
        });
    }

    private void checkPublisherCleanup(String topic) throws Exception {
        try {
            ContainerExecResult result = pulsarCluster.getAnyBroker().execCmd(
                    PulsarCluster.ADMIN_SCRIPT,
                    "topics",
                    "stats",
                    topic);
            TopicStats topicStats = ObjectMapperFactory.getMapper().reader()
                    .readValue(result.getStdout(), TopicStats.class);
            assertEquals(topicStats.getPublishers().size(), 0);

        } catch (ContainerExecException e) {
            fail("Command should have exited with non-zero");
        }
    }

    private void getFunctionStatus(String functionName, int numMessages, boolean checkRestarts) throws Exception {
        getFunctionStatus(functionName, numMessages, checkRestarts, 1);
    }

    private void getFunctionStatus(String functionName, int numMessages, boolean checkRestarts, int parallelism)
            throws Exception {
        Awaitility.await()
                .pollInterval(Duration.ofSeconds(1))
                .atMost(Duration.ofSeconds(15))
                .ignoreExceptions()
                .untilAsserted(() ->
                        doGetFunctionStatus(functionName, numMessages, checkRestarts, parallelism));
    }

    private void doGetFunctionStatus(String functionName, int numMessages, boolean checkRestarts, int parallelism)
            throws Exception {
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "status",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName
        );

        FunctionStatus functionStatus = FunctionStatusUtil.decode(result.getStdout());

        assertEquals(functionStatus.getNumInstances(), parallelism);
        assertEquals(functionStatus.getNumRunning(), parallelism);
        assertEquals(functionStatus.getInstances().size(), parallelism);
        boolean avgLatencyGreaterThanZero = false;
        int totalMessagesProcessed = 0;
        int totalMessagesSuccessfullyProcessed = 0;
        boolean lastInvocationTimeGreaterThanZero = false;
        for (int i = 0; i < parallelism; ++i) {
            assertEquals(functionStatus.getInstances().get(i).getStatus().isRunning(), true);
            assertTrue(functionStatus.getInstances().get(i).getInstanceId() >= 0);
            assertTrue(functionStatus.getInstances().get(i).getInstanceId() < parallelism);
            avgLatencyGreaterThanZero = avgLatencyGreaterThanZero
                    || functionStatus.getInstances().get(i).getStatus().getAverageLatency() > 0.0;
            lastInvocationTimeGreaterThanZero = lastInvocationTimeGreaterThanZero
                    || functionStatus.getInstances().get(i).getStatus().getLastInvocationTime() > 0;
            totalMessagesProcessed += functionStatus.getInstances().get(i).getStatus().getNumReceived();
            totalMessagesSuccessfullyProcessed +=
                    functionStatus.getInstances().get(i).getStatus().getNumSuccessfullyProcessed();
            if (checkRestarts) {
                assertEquals(functionStatus.getInstances().get(i).getStatus().getNumRestarts(), 0);
            }
            assertEquals(functionStatus.getInstances().get(i).getStatus().getLatestUserExceptions().size(), 0);
            assertEquals(functionStatus.getInstances().get(i).getStatus().getLatestSystemExceptions().size(), 0);
        }
        if (numMessages > 0) {
            assertTrue(avgLatencyGreaterThanZero);
            assertTrue(lastInvocationTimeGreaterThanZero);
        }
        assertEquals(totalMessagesProcessed, numMessages);
        assertEquals(totalMessagesSuccessfullyProcessed, numMessages);
    }

    private void publishAndConsumeMessages(String inputTopic,
                                           String outputTopic,
                                           int numMessages) throws Exception {
        @Cleanup PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(outputTopic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("test-sub")
                .subscribe();

        if (inputTopic.endsWith(".*")) {
            @Cleanup Producer<String> producer1 = client.newProducer(Schema.STRING)
                    .topic(inputTopic.substring(0, inputTopic.length() - 2) + "1")
                    .create();

            @Cleanup Producer<String> producer2 = client.newProducer(Schema.STRING)
                    .topic(inputTopic.substring(0, inputTopic.length() - 2) + "2")
                    .create();

            for (int i = 0; i < numMessages / 2; i++) {
                producer1.send("message-" + i);
            }

            for (int i = numMessages / 2; i < numMessages; i++) {
                producer2.send("message-" + i);
            }
        } else {
            @Cleanup Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(inputTopic)
                    .create();

            for (int i = 0; i < numMessages; i++) {
                producer.send("message-" + i);
            }
        }

        Set<String> expectedMessages = new HashSet<>();
        for (int i = 0; i < numMessages; i++) {
            expectedMessages.add("message-" + i + "!");
        }

        for (int i = 0; i < numMessages; i++) {
            log.info("Trying to receive message.. {}/{}", i, numMessages);
            Message<String> msg = consumer.receive(30, TimeUnit.MINUTES);
            log.info("Received: {}", msg.getValue());
            assertTrue(expectedMessages.contains(msg.getValue()));
            expectedMessages.remove(msg.getValue());
        }
    }

    private void publishAndConsumeMessagesBytes(String inputTopic,
                                                String outputTopic,
                                                int numMessages) throws Exception {
        @Cleanup PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                .topic(outputTopic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("test-sub")
                .subscribe();

        if (inputTopic.endsWith(".*")) {
            @Cleanup Producer<byte[]> producer1 = client.newProducer(Schema.BYTES)
                    .topic(inputTopic.substring(0, inputTopic.length() - 2) + "1")
                    .create();

            @Cleanup Producer<byte[]> producer2 = client.newProducer(Schema.BYTES)
                    .topic(inputTopic.substring(0, inputTopic.length() - 2) + "2")
                    .create();

            for (int i = 0; i < numMessages / 2; i++) {
                producer1.send(("message-" + i).getBytes(UTF_8));
            }

            for (int i = numMessages / 2; i < numMessages; i++) {
                producer2.send(("message-" + i).getBytes(UTF_8));
            }
        } else if (inputTopic.contains(",")) {
            String[] topics = inputTopic.split(",");
            @Cleanup Producer<byte[]> producer1 = client.newProducer(Schema.BYTES)
                    .topic(topics[0])
                    .create();

            @Cleanup Producer<byte[]> producer2 = client.newProducer(Schema.BYTES)
                    .topic(topics[1])
                    .create();

            for (int i = 0; i < numMessages / 2; i++) {
                producer1.send(("message-" + i).getBytes(UTF_8));
            }

            for (int i = numMessages / 2; i < numMessages; i++) {
                producer2.send(("message-" + i).getBytes(UTF_8));
            }
        } else {
            @Cleanup Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                    .topic(inputTopic)
                    .create();

            for (int i = 0; i < numMessages; i++) {
                producer.send(("message-" + i).getBytes(UTF_8));
            }
        }

        Set<String> expectedMessages = new HashSet<>();
        for (int i = 0; i < numMessages; i++) {
            expectedMessages.add("message-" + i + "!");
        }

        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> msg = consumer.receive(30, TimeUnit.SECONDS);
            String msgValue = new String(msg.getValue(), UTF_8);
            log.info("Received: {}", msgValue);
            assertTrue(expectedMessages.contains(msgValue));
            expectedMessages.remove(msgValue);
        }
    }

    private void deleteFunction(String functionName) throws Exception {
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "delete",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName
        );
        assertTrue(result.getStdout().contains("Deleted successfully"));
        result.assertNoStderr();
    }

    protected void testAutoSchemaFunction() throws Exception {
        String inputTopicName = "test-autoschema-input-" + randomName(8);
        String outputTopicName = "test-autoschema-output-" + randomName(8);
        String functionName = "test-autoschema-fn-" + randomName(8);
        final int numMessages = 10;


        // submit the exclamation function
        submitFunction(
                Runtime.JAVA,
                inputTopicName,
                outputTopicName,
                functionName,
                false,
                false,
                false,
                AutoSchemaFunction.class.getName(),
                Schema.AVRO(CustomObject.class),
                null);

        // get function info
        getFunctionInfoSuccess(functionName);

        // publish and consume result
        publishAndConsumeAvroMessages(inputTopicName, outputTopicName, numMessages);

        // get function status. Note that this function might restart a few times until
        // the producer above writes the messages.
        getFunctionStatus(functionName, numMessages, false);

        // delete function
        deleteFunction(functionName);

        // get function info
        getFunctionInfoNotFound(functionName);
    }

    private void publishAndConsumeAvroMessages(String inputTopic,
                                               String outputTopic,
                                               int numMessages) throws Exception {

        @Cleanup PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(outputTopic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("test-sub")
                .subscribe();

        @Cleanup Producer<CustomObject> producer = client.newProducer(Schema.AVRO(CustomObject.class))
                .topic(inputTopic)
                .create();

        for (int i = 0; i < numMessages; i++) {
            CustomObject co = new CustomObject(i);
            producer.send(co);
        }

        for (int i = 0; i < numMessages; i++) {
            Message<String> msg = consumer.receive();
            assertEquals("value-" + i, msg.getValue());
        }
    }

    protected void testAvroSchemaFunction(Runtime runtime) throws Exception {
        if (functionRuntimeType == FunctionRuntimeType.THREAD && runtime == Runtime.PYTHON) {
            // python can only run on process mode
            return;
        }

        log.info("testAvroSchemaFunction start ...");
        final String inputTopic = "test-avroschema-input-" + randomName(8);
        final String outputTopic = "test-avroschema-output-" + randomName(8);
        final String functionName = "test-avroschema-fn-" + randomName(8);
        final int numMessages = 10;

        @Cleanup PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl()).build();
        log.info("pulsar client init - input: {}, output: {}", inputTopic, outputTopic);

        @Cleanup Producer<AvroTestObject> producer = pulsarClient
                .newProducer(Schema.AVRO(AvroTestObject.class))
                .topic(inputTopic).create();
        log.info("pulsar producer init - {}", inputTopic);

        @Cleanup Consumer<AvroTestObject> consumer = pulsarClient
                .newConsumer(Schema.AVRO(AvroTestObject.class))
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("test-avro-schema")
                .topic(outputTopic)
                .subscribe();
        log.info("pulsar consumer init - {}", outputTopic);

        CompletableFuture<Optional<SchemaInfo>> inputSchemaFuture =
                ((PulsarClientImpl) pulsarClient).getSchema(inputTopic);
        inputSchemaFuture.whenComplete((schemaInfo, throwable) -> {
            if (schemaInfo.isPresent()) {
                log.info("inputSchemaInfo: {}", schemaInfo.get().toString());
            } else {
                log.error("input schema is not present!");
            }
        });

        CompletableFuture<Optional<SchemaInfo>> outputSchemaFuture =
                ((PulsarClientImpl) pulsarClient).getSchema(outputTopic);
        outputSchemaFuture.whenComplete((schemaInfo, throwable) -> {
            if (throwable != null) {
                log.error("get output schemaInfo error", throwable);
                throwable.printStackTrace();
                return;
            }
            if (schemaInfo.isPresent()) {
                log.info("outputSchemaInfo: {}", schemaInfo.get().toString());
            } else {
                log.error("output schema is not present!");
            }
        });

        if (runtime == Runtime.JAVA) {
            submitFunction(
                    Runtime.JAVA,
                    inputTopic,
                    outputTopic,
                    functionName,
                    null,
                    AvroSchemaTestFunction.class.getName(),
                    Schema.AVRO(AvroTestObject.class),
                    null);
        } else if (runtime == Runtime.PYTHON) {
            ConsumerConfig consumerConfig = new ConsumerConfig();
            consumerConfig.setSchemaType("avro");
            Map<String, String> inputSpecs = new HashMap<>() {{
                put(inputTopic, objectMapper.writeValueAsString(consumerConfig));
            }};
            submitFunction(
                    Runtime.PYTHON,
                    inputTopic,
                    outputTopic,
                    functionName,
                    AVRO_SCHEMA_FUNCTION_PYTHON_FILE,
                    AVRO_SCHEMA_PYTHON_CLASS,
                    Schema.AVRO(AvroTestObject.class),
                    null, objectMapper.writeValueAsString(inputSpecs), "avro", null,
                    "avro_schema_test_function.AvroTestObject", "avro_schema_test_function.AvroTestObject",
                    null);
        }
        log.info("pulsar submitFunction");

        getFunctionInfoSuccess(functionName);

        AvroSchemaTestFunction function = new AvroSchemaTestFunction();
        Set<Object> expectedSet = new HashSet<>();

        log.info("test-avro-schema producer connected: " + producer.isConnected());
        for (int i = 0; i < numMessages; i++) {
            AvroTestObject inputObject = new AvroTestObject();
            inputObject.setBaseValue(i);
            MessageId messageId = producer.send(inputObject);
            log.info("test-avro-schema messageId: {}", messageId.toString());
            expectedSet.add(function.process(inputObject, null));
            log.info("test-avro-schema expectedSet size: {}", expectedSet.size());
        }
        getFunctionStatus(functionName, numMessages, false);
        log.info("test-avro-schema producer send message finish");

        CompletableFuture<Optional<SchemaInfo>> outputSchemaFuture2 =
                ((PulsarClientImpl) pulsarClient).getSchema(outputTopic);
        outputSchemaFuture2.whenComplete((schemaInfo, throwable) -> {
            if (throwable != null) {
                log.error("get output schemaInfo error", throwable);
                throwable.printStackTrace();
                return;
            }
            if (schemaInfo.isPresent()) {
                log.info("outputSchemaInfo: {}", schemaInfo.get().toString());
            } else {
                log.error("output schema is not present!");
            }
        });

        log.info("test-avro-schema consumer connected: " + consumer.isConnected());
        for (int i = 0; i < numMessages; i++) {
            log.info("test-avro-schema consumer receive [{}] start", i);
            Message<AvroTestObject> message = consumer.receive();
            log.info("test-avro-schema consumer receive [{}] over", i);
            AvroTestObject outputObject = message.getValue();
            assertTrue(expectedSet.contains(outputObject));
            expectedSet.remove(outputObject);
            consumer.acknowledge(message);
        }
        log.info("test-avro-schema consumer receive message finish");

        assertEquals(expectedSet.size(), 0);

        deleteFunction(functionName);

        getFunctionInfoNotFound(functionName);
    }


    protected void testInitFunction(Runtime runtime) throws Exception {
        if (runtime != Runtime.JAVA) {
            // only java support init function
            return;
        }

        Schema<?> schema = Schema.STRING;

        String inputTopicName = "persistent://public/default/test-init-" + runtime + "-input-" + randomName(8);
        String outputTopicName = "test-init-" + runtime + "-output-" + randomName(8);
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {
            admin.topics().createNonPartitionedTopic(inputTopicName);
            admin.topics().createNonPartitionedTopic(outputTopicName);
        }

        String functionName = "test-init-fn-" + randomName(8);
        final int numMessages = 10;

        // submit the exclamation function
        submitFunction(runtime, inputTopicName, outputTopicName, functionName, null,
                InitializableFunction.class.getName(), schema,
                Collections.singletonMap("publish-topic", outputTopicName), null, null, null, null, null, null);

        // publish and consume result
        publishAndConsumeMessages(inputTopicName, outputTopicName, numMessages);

        // delete function
        deleteFunction(functionName);
    }

    protected void testLoggingFunction(Runtime runtime) throws Exception {
        if (functionRuntimeType == FunctionRuntimeType.THREAD && runtime == Runtime.PYTHON) {
            // python can only run on process mode
            return;
        }

        if (functionRuntimeType == FunctionRuntimeType.THREAD && runtime == Runtime.GO) {
            // go can only run on process mode
            return;
        }


        Schema<?> schema;
        if (Runtime.JAVA == runtime) {
            schema = Schema.STRING;
        } else {
            schema = Schema.BYTES;
        }

        String inputTopicName = "persistent://public/default/test-log-" + runtime + "-input-" + randomName(8);
        String logTopicName = "test-log-" + runtime + "-log-topic-" + randomName(8);
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {
            admin.topics().createNonPartitionedTopic(inputTopicName);
            admin.topics().createNonPartitionedTopic(logTopicName);
        }

        String functionName = "test-logging-fn-" + randomName(8);
        final int numMessages = 10;

        // submit the exclamation function
        submitJavaLoggingFunction(
                inputTopicName, logTopicName, functionName, schema);

        // get function info
        getFunctionInfoSuccess(functionName);

        // get function stats
        getFunctionStatsEmpty(functionName);

        try {
            // publish and consume result
            publishAndConsumeMessages(inputTopicName, logTopicName, numMessages, "-log");
        } finally {
            // dump function logs so that it's easier to investigate failures
            pulsarCluster.dumpFunctionLogs(functionName);
        }

        // get function status
        getFunctionStatus(functionName, numMessages, true);

        // get function stats
        getFunctionStats(functionName, numMessages);

        // delete function
        deleteFunction(functionName);

        // get function info
        getFunctionInfoNotFound(functionName);

        // make sure subscriptions are cleanup
        checkSubscriptionsCleanup(inputTopicName);
        checkPublisherCleanup(logTopicName);

    }

    private void submitJavaLoggingFunction(String inputTopicName,
                                           String logTopicName,
                                           String functionName,
                                           Schema<?> schema) throws Exception {
        ensureSubscriptionCreated(inputTopicName, String.format("public/default/%s", functionName), schema);

        CommandGenerator generator;
        log.info("------- INPUT TOPIC: '{}'", inputTopicName);
        if (inputTopicName.endsWith(".*")) {
            log.info("----- CREATING TOPIC PATTERN FUNCTION --- ");
            generator = CommandGenerator.createTopicPatternGenerator(inputTopicName, LOGGING_JAVA_CLASS);
        } else {
            log.info("----- CREATING REGULAR FUNCTION --- ");
            generator = CommandGenerator.createDefaultGenerator(inputTopicName, LOGGING_JAVA_CLASS);
        }
        generator.setLogTopic(logTopicName);
        generator.setFunctionName(functionName);
        String command = generator.generateCreateFunctionCommand();

        log.info("---------- Function command: {}", command);
        String[] commands = {
                "sh", "-c", command
        };
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                commands);
        assertTrue(result.getStdout().contains("Created successfully"));
    }

    private void publishAndConsumeMessages(String inputTopic,
                                           String outputTopic,
                                           int numMessages,
                                           String messagePostfix) throws Exception {
        @Cleanup PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup Consumer<byte[]> consumer = client.newConsumer()
                .topic(outputTopic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("test-sub")
                .subscribe();

        @Cleanup Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(inputTopic)
                .create();

        for (int i = 0; i < numMessages; i++) {
            producer.send("message-" + i);
        }

        Set<String> expectedMessages = new HashSet<>();
        for (int i = 0; i < numMessages; i++) {
            expectedMessages.add("message-" + i + messagePostfix);
        }

        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> msg = consumer.receive(30, TimeUnit.SECONDS);
            if (msg == null) {
                log.info("Input topic stats: {}",
                        objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(
                                pulsarAdmin.topics().getStats(inputTopic, true)));
                log.info("Output topic stats: {}",
                        objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(
                                pulsarAdmin.topics().getStats(outputTopic, true)));
                log.info("Input topic internal-stats: {}",
                        objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(
                                pulsarAdmin.topics().getInternalStats(inputTopic, true)));
                log.info("Output topic internal-stats: {}",
                        objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(
                                pulsarAdmin.topics().getInternalStats(outputTopic, true)));
            } else {
                String logMsg = new String(msg.getValue(), UTF_8);
                log.info("Received message: '{}'", logMsg);
                assertTrue(expectedMessages.contains(logMsg), "Message '" + logMsg + "' not expected");
                expectedMessages.remove(logMsg);
            }
        }

        consumer.close();
        producer.close();
        client.close();
    }


    protected void testGenericObjectFunction(String function, boolean removeAgeField, boolean keyValue)
            throws Exception {
        log.info("start {} function test ...", function);

        String ns = "public/ns-genericobject-" + randomName(8);
        @Cleanup
        PulsarAdmin pulsarAdmin = getPulsarAdmin();
        pulsarAdmin.namespaces().createNamespace(ns);

        @Cleanup
        PulsarClient pulsarClient = getPulsarClient();

        final int numMessages = 10;
        final String inputTopic = ns + "/test-object-input-" + randomName(8);
        final String outputTopic = ns + "/test-object-output" + randomName(8);
        @Cleanup
        Consumer<GenericRecord> consumer = pulsarClient
                .newConsumer(Schema.AUTO_CONSUME())
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .topic(outputTopic)
                .subscribe();

        final String functionName = "test-generic-fn-" + randomName(8);
        submitFunction(
                Runtime.JAVA,
                inputTopic,
                outputTopic,
                functionName,
                null,
                function,
                Schema.AUTO_CONSUME(),
                null,
                null,
                SchemaType.NONE.name(),
                SubscriptionInitialPosition.Earliest, null, null, null);
        try {
            if (keyValue) {
                @Cleanup
                Producer<KeyValue<Users.UserV1, Users.UserV1>> producer = pulsarClient
                        .newProducer(Schema.KeyValue(
                                Schema.AVRO(Users.UserV1.class),
                                Schema.AVRO(Users.UserV1.class), KeyValueEncodingType.SEPARATED))
                        .topic(inputTopic)
                        .create();
                for (int i = 0; i < numMessages; i++) {
                    producer.send(new KeyValue<>(new Users.UserV1("foo" + i, i),
                            new Users.UserV1("bar" + i, i + 100)));
                }
            } else {
                @Cleanup
                Producer<Users.UserV1> producer = pulsarClient
                        .newProducer(Schema.AVRO(Users.UserV1.class))
                        .topic(inputTopic)
                        .create();
                for (int i = 0; i < numMessages; i++) {
                    producer.send(new Users.UserV1("bar" + i, i + 100));
                }
            }

            getFunctionInfoSuccess(functionName);

            getFunctionStatus(functionName, numMessages, true);

            int i = 0;
            Message<GenericRecord> message;
            do {
                message = consumer.receive(30, TimeUnit.SECONDS);
                if (message != null) {
                    GenericRecord genericRecord = message.getValue();
                    if (keyValue) {
                        @SuppressWarnings("unchecked")
                        KeyValue<GenericRecord, GenericRecord> keyValueObject =
                                (KeyValue<GenericRecord, GenericRecord>) genericRecord.getNativeObject();
                        GenericRecord key = keyValueObject.getKey();
                        GenericRecord value = keyValueObject.getValue();
                        key.getFields().forEach(f -> {
                            log.info("key field {} value {}", f.getName(), key.getField(f.getName()));
                        });
                        value.getFields().forEach(f -> {
                            log.info("value field {} value {}", f.getName(), value.getField(f.getName()));
                        });
                        assertEquals(i, key.getField("age"));
                        assertEquals("foo" + i, key.getField("name"));

                        if (removeAgeField) {
                            // field "age" is removed from the schema
                            assertFalse(value.getFields().stream().anyMatch(f -> f.getName().equals("age")));
                        } else {
                            assertEquals(i + 100, value.getField("age"));
                        }
                        assertEquals("bar" + i, value.getField("name"));
                    } else {
                        GenericRecord value = genericRecord;
                        log.info("received value {}", value);
                        value.getFields().forEach(f -> {
                            log.info("value field {} value {}", f.getName(), value.getField(f.getName()));
                        });

                        if (removeAgeField) {
                            // field "age" is removed from the schema
                            assertFalse(value.getFields().stream().anyMatch(f -> f.getName().equals("age")));
                        } else {
                            assertEquals(i + 100, value.getField("age"));
                        }
                        assertEquals("bar" + i, value.getField("name"));
                    }

                    consumer.acknowledge(message);
                    i++;
                }
            } while (message != null);
        } finally {
            pulsarCluster.dumpFunctionLogs(functionName);
        }

        deleteFunction(functionName);

        getFunctionInfoNotFound(functionName);
    }

    protected void testRecordFunction() throws Exception {
        log.info("start RecordFunction function test ...");

        String ns = "public/ns-recordfunction-" + randomName(8);
        @Cleanup
        PulsarAdmin pulsarAdmin = getPulsarAdmin();
        pulsarAdmin.namespaces().createNamespace(ns);

        @Cleanup
        PulsarClient pulsarClient = getPulsarClient();

        final int numMessages = 10;
        final String inputTopic = ns + "/test-string-input-" + randomName(8);
        final String outputTopic = ns + "/test-string-output-" + randomName(8);
        @Cleanup
        Consumer<String> consumer = pulsarClient
                .newConsumer(Schema.STRING)
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .topic("publishtopic")
                .subscribe();

        final String functionName = "test-record-fn-" + randomName(8);
        submitFunction(
                Runtime.JAVA,
                inputTopic,
                outputTopic,
                functionName,
                null,
                RecordFunction.class.getName(),
                Schema.AUTO_CONSUME(),
                null);
        try {
            @Cleanup
            Producer<String> producer = pulsarClient
                    .newProducer(Schema.STRING)
                    .topic(inputTopic)
                    .create();
            for (int i = 0; i < numMessages; i++) {
                producer.send("message" + i);
            }

            getFunctionInfoSuccess(functionName);

            getFunctionStatus(functionName, numMessages, true);

            for (int i = 0; i < numMessages; i++) {
                Message<String> msg = consumer.receive(30, TimeUnit.SECONDS);
                log.info("Received: {}", msg.getValue());
                assertEquals(msg.getValue(), "message" + i + "!");
                assertEquals(msg.getProperty("input_topic"), "persistent://" + inputTopic);
            }
        } finally {
            pulsarCluster.dumpFunctionLogs(functionName);
        }

        deleteFunction(functionName);

        getFunctionInfoNotFound(functionName);
    }

    protected void testMergeFunction() throws Exception {
        log.info("start merge function test ...");

        String ns = "public/ns-merge-" + randomName(8);
        @Cleanup
        PulsarAdmin pulsarAdmin = getPulsarAdmin();
        pulsarAdmin.namespaces().createNamespace(ns);
        pulsarAdmin.namespaces().setSchemaCompatibilityStrategy(ns, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        SchemaCompatibilityStrategy strategy = pulsarAdmin.namespaces().getSchemaCompatibilityStrategy(ns);
        log.info("namespace {} SchemaCompatibilityStrategy is {}", ns, strategy);

        @Cleanup
        PulsarClient pulsarClient = getPulsarClient();

        ObjectNode inputSpecNode = objectMapper.createObjectNode();
        Map<String, AtomicInteger> topicMsgCntMap = new ConcurrentHashMap<>();
        int messagePerTopic = 10;
        prepareDataForMergeFunction(ns, pulsarClient, inputSpecNode, messagePerTopic, topicMsgCntMap);

        final String outputTopic = ns + "/test-merge-output";
        @Cleanup
        Consumer<GenericRecord> consumer = pulsarClient
                .newConsumer(Schema.AUTO_CONSUME())
                .subscriptionName("test-merge-fn")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .topic(outputTopic)
                .subscribe();

        final String functionName = "test-merge-fn-" + randomName(8);
        submitFunction(
                Runtime.JAVA,
                "",
                outputTopic,
                functionName,
                null,
                MergeTopicFunction.class.getName(),
                null,
                null,
                inputSpecNode.toString(),
                SchemaType.AUTO_PUBLISH.name().toUpperCase(),
                SubscriptionInitialPosition.Earliest, null, null, null);

        getFunctionInfoSuccess(functionName);

        getFunctionStatus(functionName, topicMsgCntMap.keySet().size() * messagePerTopic, true);

        try {

            Message<GenericRecord> message;
            do {
                message = consumer.receive(30, TimeUnit.SECONDS);
                if (message != null) {
                    String baseTopic = message.getProperty("baseTopic");
                    GenericRecord genericRecord = message.getValue();
                    log.info("receive msg baseTopic: {}, schemaType: {}, nativeClass: {}, nativeObject: {}",
                            baseTopic,
                            genericRecord.getSchemaType(),
                            genericRecord.getNativeObject().getClass(),
                            genericRecord.getNativeObject());
                    checkSchemaForAutoSchema(message, baseTopic);
                    topicMsgCntMap.get(baseTopic).decrementAndGet();
                    consumer.acknowledge(message);
                }
            } while (message != null);

            for (Map.Entry<String, AtomicInteger> entry : topicMsgCntMap.entrySet()) {
                assertEquals(entry.getValue().get(), 0,
                        "topic " + entry.getKey() + " left message cnt is not 0.");
            }
        } finally {
            pulsarCluster.dumpFunctionLogs(functionName);
        }

        deleteFunction(functionName);

        getFunctionInfoNotFound(functionName);
        log.info("finish merge function test.");
    }

    private void prepareDataForMergeFunction(String ns,
                                             PulsarClient pulsarClient,
                                             ObjectNode inputSpecNode,
                                             int messagePerTopic,
                                             Map<String, AtomicInteger> topicMsgCntMap) throws PulsarClientException {
        generateDataByDifferentSchema(ns, "merge-schema-bytes", pulsarClient,
                Schema.BYTES, "bytes schema test".getBytes(), messagePerTopic, inputSpecNode, topicMsgCntMap);
        generateDataByDifferentSchema(ns, "merge-schema-string", pulsarClient,
                Schema.STRING, "string schema test", messagePerTopic, inputSpecNode, topicMsgCntMap);
        generateDataByDifferentSchema(ns, "merge-schema-json-userv1", pulsarClient,
                Schema.JSON(Users.UserV1.class), new Users.UserV1("ran", 33),
                messagePerTopic, inputSpecNode, topicMsgCntMap);
        generateDataByDifferentSchema(ns, "merge-schema-json-userv2", pulsarClient,
                Schema.JSON(Users.UserV2.class), new Users.UserV2("tang", 18, "123123123"),
                messagePerTopic, inputSpecNode, topicMsgCntMap);
        generateDataByDifferentSchema(ns, "merge-schema-avro-userv2", pulsarClient,
                Schema.AVRO(Users.UserV2.class), new Users.UserV2("tang", 20, "456456456"),
                messagePerTopic, inputSpecNode, topicMsgCntMap);
        generateDataByDifferentSchema(ns, "merge-schema-k-int-v-json-userv1-separate", pulsarClient,
                Schema.KeyValue(Schema.INT32, Schema.JSON(Users.UserV1.class), KeyValueEncodingType.SEPARATED),
                new KeyValue<>(100, new Users.UserV1("ran", 40)),
                messagePerTopic, inputSpecNode, topicMsgCntMap);
        generateDataByDifferentSchema(ns, "merge-schema-k-json-userv2-v-json-userv1-inline", pulsarClient,
                Schema.KeyValue(Schema.JSON(Users.UserV2.class), Schema.JSON(Users.UserV1.class),
                        KeyValueEncodingType.INLINE),
                new KeyValue<>(new Users.UserV2("tang", 20, "789789789"),
                        new Users.UserV1("ran", 40)),
                messagePerTopic, inputSpecNode, topicMsgCntMap);
    }

    private <T> void generateDataByDifferentSchema(String ns,
                                                   String baseTopic,
                                                   PulsarClient pulsarClient,
                                                   Schema<T> schema,
                                                   T data,
                                                   int messageCnt,
                                                   ObjectNode inputSpecNode,
                                                   Map<String, AtomicInteger> topicMsgCntMap)
            throws PulsarClientException {
        String topic = ns + "/" + baseTopic;
        Producer<T> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .create();
        for (int i = 0; i < messageCnt; i++) {
            producer.newMessage().value(data).property("baseTopic", baseTopic).send();
        }
        ObjectNode confNode = objectMapper.createObjectNode();
        confNode.put("schemaType", SchemaType.AUTO_CONSUME.name().toUpperCase());
        inputSpecNode.put(topic, confNode.toString());
        topicMsgCntMap.put(baseTopic, new AtomicInteger(messageCnt));
        producer.close();
        log.info("[merge-fn] generate {} messages for schema {}", messageCnt, schema.getSchemaInfo());
    }

    @SuppressWarnings("unchecked")
    private void checkSchemaForAutoSchema(Message<GenericRecord> message, String baseTopic) {
        if (message.getReaderSchema().isEmpty()) {
            fail("Failed to get reader schema for auto consume multiple schema topic.");
        }
        Object nativeObject = message.getValue().getNativeObject();
        JsonNode jsonNode;
        KeyValue<?, ?> kv;
        switch (baseTopic) {
            case "merge-schema-bytes":
                assertEquals(new String((byte[]) nativeObject), "bytes schema test");
                break;
            case "merge-schema-string":
                assertEquals((String) nativeObject, "string schema test");
                break;
            case "merge-schema-json-userv1":
                jsonNode = (JsonNode) nativeObject;
                assertEquals(jsonNode.get("name").textValue(), "ran");
                assertEquals(jsonNode.get("age").intValue(), 33);
                break;
            case "merge-schema-json-userv2":
                jsonNode = (JsonNode) nativeObject;
                assertEquals(jsonNode.get("name").textValue(), "tang");
                assertEquals(jsonNode.get("age").intValue(), 18);
                assertEquals(jsonNode.get("phone").textValue(), "123123123");
                break;
            case "merge-schema-avro-userv2":
                org.apache.avro.generic.GenericRecord genericRecord =
                        (org.apache.avro.generic.GenericRecord) nativeObject;
                assertEquals(genericRecord.get("name").toString(), "tang");
                assertEquals(genericRecord.get("age"), 20);
                assertEquals(genericRecord.get("phone").toString(), "456456456");
                break;
            case "merge-schema-k-int-v-json-userv1-separate":
                kv = (KeyValue<Integer, GenericRecord>) nativeObject;
                assertEquals(kv.getKey(), 100);
                jsonNode = ((GenericJsonRecord) kv.getValue()).getJsonNode();
                assertEquals(jsonNode.get("name").textValue(), "ran");
                assertEquals(jsonNode.get("age").intValue(), 40);
                break;
            case "merge-schema-k-json-userv2-v-json-userv1-inline":
                kv = (KeyValue<GenericRecord, GenericRecord>) nativeObject;
                jsonNode = ((GenericJsonRecord) kv.getKey()).getJsonNode();
                assertEquals(jsonNode.get("name").textValue(), "tang");
                assertEquals(jsonNode.get("age").intValue(), 20);
                assertEquals(jsonNode.get("phone").textValue(), "789789789");
                jsonNode = ((GenericJsonRecord) kv.getValue()).getJsonNode();
                assertEquals(jsonNode.get("name").textValue(), "ran");
                assertEquals(jsonNode.get("age").intValue(), 40);
                break;
            default:
                // nothing to do
        }
    }

    private PulsarClient getPulsarClient() throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(pulsarCluster.getPlainTextServiceUrl()).build();
    }

    private PulsarAdmin getPulsarAdmin() throws PulsarClientException {
        return PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();
    }
}
