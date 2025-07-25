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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.util.HashedWheelTimer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBusyException;
import org.apache.pulsar.client.api.PulsarClientException.ProducerFencedException;
import org.apache.pulsar.client.api.PulsarClientException.TimeoutException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ExclusiveProducerTest extends BrokerTestBase {

    @BeforeClass
    protected void setup() throws Exception {
        baseSetup();
    }

    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @DataProvider(name = "topics")
    public static Object[][] topics() {
        return new Object[][] {
                { "persistent", Boolean.TRUE },
                { "persistent", Boolean.FALSE },
                { "non-persistent", Boolean.TRUE },
                { "non-persistent", Boolean.FALSE },
        };
    }

    @DataProvider(name = "accessMode")
    public static Object[][] accessMode() {
        return new Object[][] {
                // ProducerAccessMode, partitioned
                { ProducerAccessMode.Exclusive, Boolean.TRUE},
                { ProducerAccessMode.Exclusive, Boolean.FALSE },
                { ProducerAccessMode.ExclusiveWithFencing, Boolean.TRUE},
                { ProducerAccessMode.ExclusiveWithFencing, Boolean.FALSE },
                { ProducerAccessMode.WaitForExclusive, Boolean.TRUE },
                { ProducerAccessMode.WaitForExclusive, Boolean.FALSE }
        };
    }

    @Test(dataProvider = "topics")
    public void simpleTest(String type, boolean partitioned) throws Exception {
        String topic = newTopic(type, partitioned);
        simpleTest(topic);
    }

    private void simpleTest(String topic) throws Exception {

        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .producerName("p1")
                .accessMode(ProducerAccessMode.Exclusive)
                .create();

        try {
            pulsarClient.newProducer(Schema.STRING)
                    .topic(topic)
                    .producerName("p-fail-1")
                    .accessMode(ProducerAccessMode.Exclusive)
                    .create();
            fail("Should have failed");
        } catch (ProducerFencedException e) {
            // Expected
        }

        try {
            pulsarClient.newProducer(Schema.STRING)
                    .topic(topic)
                    .producerName("p-fail-2")
                    .accessMode(ProducerAccessMode.Shared)
                    .create();
            fail("Should have failed");
        } catch (ProducerBusyException e) {
            // Expected
        }

        p1.close();

        // Now producer should be allowed to get in
        Producer<String> p2 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .producerName("p2")
                .accessMode(ProducerAccessMode.Exclusive)
                .create();

        Producer<String> p3 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .producerName("p3")
                .accessMode(ProducerAccessMode.ExclusiveWithFencing)
                .create();

        try {
            p2.send("test");
            fail("Should have failed");
        } catch (ProducerFencedException expected) {
        }

        // this should work
        p3.send("test");
        p3.close();

        // test now WaitForExclusive vs ExclusiveWithFencing

        // use two different Clients, because sometimes fencing a Producer triggers connection close
        // making the test unreliable.

        @Cleanup
        PulsarClient pulsarClient2 = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .operationTimeout(2, TimeUnit.SECONDS)
                .build();

        Producer<String> p4 = pulsarClient2.newProducer(Schema.STRING)
                .topic(topic)
                .producerName("p4")
                .accessMode(ProducerAccessMode.Exclusive)
                .create();

        p4.send("test");

        // p5 will be waiting for the lock to be released
        CompletableFuture<Producer<String>> p5 = pulsarClient2.newProducer(Schema.STRING)
                .topic(topic)
                .producerName("p5")
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .createAsync();

        // wait for all the Producers to be enqueued in order to prevent races
        Thread.sleep(2000);

        // p6 fences out all the current producers, even p5
        Producer<String> p6 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .producerName("p6")
                .accessMode(ProducerAccessMode.ExclusiveWithFencing)
                .create();

        p6.send("test");

        // p7 is enqueued after p6
        CompletableFuture<Producer<String>> p7 = pulsarClient2.newProducer(Schema.STRING)
                .topic(topic)
                .producerName("p7")
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .createAsync();

        // this should work, p6 is the owner
        p6.send("test");

        try {
            p4.send("test");
            fail("Should have failed");
        } catch (ProducerFencedException expected) {
        }

        // this should work, p6 is the owner
        p6.send("test");

        // p5 fails
        try {
            p5.get();
            fail("Should have failed");
        } catch (ExecutionException expected) {
            assertTrue(expected.getCause() instanceof ProducerFencedException,
                    "unexpected exception " + expected.getCause());
        }

        // this should work, p6 is the owner
        p6.send("test");

        p6.close();

        // p7 finally acquires the lock
        p7.get().send("test");
        p7.get().close();
    }

    @Test(dataProvider = "topics")
    public void testProducerTasksCleanupWhenUsingExclusiveProducers(String type, boolean partitioned) throws Exception {
        String topic = newTopic(type, partitioned);
        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .accessMode(ProducerAccessMode.Exclusive)
                .create();

        try {
            pulsarClient.newProducer(Schema.STRING)
                    .topic(topic)
                    .accessMode(ProducerAccessMode.Exclusive)
                    .create();
            fail("Should have failed");
        } catch (ProducerFencedException e) {
            // Expected
        }

        p1.close();

        HashedWheelTimer timer = (HashedWheelTimer) ((PulsarClientImpl) pulsarClient).timer();
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(timer.pendingTimeouts(), 0));
    }

    @Test(dataProvider = "topics")
    public void testSharedProducerCloseWithWaitForExclusiveProducer(String type, boolean partitioned) throws Exception {
        String topic = newTopic(type, partitioned);
        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .accessMode(ProducerAccessMode.Shared)
                .create();
        Producer<String> p2 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .accessMode(ProducerAccessMode.Shared)
                .create();
        CompletableFuture<Producer<String>> p3Future = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .createAsync();
        // sleep 1 second to make sure p1, p2, p3Future are added to broker producers and waitingExclusiveProducers
        Thread.sleep(1000L);
        // now p3Future is still pending because p1,p2 are active in shared mode.
        assertFalse(p3Future.isDone());
        p1.close();
        p2.close();
        // close p1,p2 and p3Future should be done and return a producer.
        Awaitility.await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(p3Future.isDone());
            assertTrue(p3Future.get().isConnected());
        });
        p3Future.get().close();
    }

    @Test(dataProvider = "topics")
    public void existingSharedProducer(String type, boolean partitioned) throws Exception {
        String topic = newTopic(type, partitioned);

        @Cleanup
        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .accessMode(ProducerAccessMode.Shared)
                .create();

        try {
            pulsarClient.newProducer(Schema.STRING)
                    .topic(topic)
                    .accessMode(ProducerAccessMode.Exclusive)
                    .create();
            fail("Should have failed");
        } catch (ProducerFencedException e) {
            // Expected
        }
    }

    @Test(dataProvider = "accessMode")
    public void producerReconnection(ProducerAccessMode accessMode, boolean partitioned) throws Exception {
        String topic = newTopic("persistent", partitioned);

        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .accessMode(accessMode)
                .create();

        p1.send("msg-1");

        admin.topics().unload(topic);

        p1.send("msg-2");
    }

    @Test(dataProvider = "accessMode")
    public void producerFenced(ProducerAccessMode accessMode, boolean partitioned) throws Exception {
        String topic = newTopic("persistent", partitioned);

        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .accessMode(accessMode)
                .create();

        p1.send("msg-1");

        // Simulate a producer that takes over and fences p1 through the topic epoch
        if (!partitioned) {
            Topic t = pulsar.getBrokerService().getTopic(topic, false).get().get();
            CompletableFuture<?> f = ((AbstractTopic) t).incrementTopicEpoch(Optional.of(0L));
            f.get();
        } else {
            for (int i = 0; i < 3; i++) {
                String name = TopicName.get(topic).getPartition(i).toString();
                Topic t = pulsar.getBrokerService().getTopic(name, false).get().get();
                CompletableFuture<?> f = ((AbstractTopic) t).incrementTopicEpoch(Optional.of(0L));
                f.get();
            }
        }

        admin.topics().unload(topic);

        try {
            p1.send("msg-2");
            fail("Should have failed");
        } catch (ProducerFencedException e) {
            // Expected
        }
    }

    @Test(dataProvider = "topics")
    public void topicDeleted(String ignored, boolean partitioned) throws Exception {
        String topic = newTopic("persistent", partitioned);

        @Cleanup
        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .accessMode(ProducerAccessMode.Exclusive)
                .create();

        p1.send("msg-1");

        if (partitioned) {
            admin.topics().deletePartitionedTopic(topic, true);
        } else {
            admin.topics().delete(topic, true);
        }

        if (!partitioned) {
            // The producer should be able to publish again on the topic
            p1.send("msg-2");
        } else {
            // The partitioned topic is deleted, the producer should not be able to publish again on the topic.
            // Partitioned metadata is required to publish messages to the topic.
            assertThrows(TimeoutException.class, () -> p1.send("msg-2"));
        }
    }

    @Test(dataProvider = "topics")
    public void waitForExclusiveTest(String type, boolean partitioned) throws Exception {
        String topic = newTopic(type, partitioned);

        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .producerName("p1")
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .create();

        CompletableFuture<Producer<String>> fp2 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .producerName("p2")
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .createAsync();

        // This sleep is made to ensure P2 is enqueued before P3. Because of the lookups
        // there's no strict guarantee they would be attempted in the same order otherwise
        Thread.sleep(1000);

        CompletableFuture<Producer<String>> fp3 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .producerName("p3")
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .createAsync();

        Thread.sleep(1000);
        // The second producer should get queued
        assertFalse(fp2.isDone());
        assertFalse(fp3.isDone());

        p1.close();

        // Now P2 should get created
        Producer<String> p2 = fp2.get(1, TimeUnit.SECONDS);

        assertFalse(fp3.isDone());

        p2.close();

        // Now P3 should get created
        Producer<String> p3 = fp3.get(1, TimeUnit.SECONDS);
        p3.close();
    }

    @Test(dataProvider = "topics")
    public void waitForExclusiveWithClientTimeout(String type, boolean partitioned) throws Exception {
        String topic = newTopic(type, partitioned);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .operationTimeout(1, TimeUnit.SECONDS)
                .build();

        Producer<String> p1 = client.newProducer(Schema.STRING)
                .topic(topic)
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .create();

        CompletableFuture<Producer<String>> fp2 = client.newProducer(Schema.STRING)
                .topic(topic)
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .createAsync();

        // Wait enough time to have caused an operation timeout on p2
        Thread.sleep(2000);

        // There should be timeout error, since the broker should reply immediately
        // with the instruction to wait
        assertFalse(fp2.isDone());

        p1.close();

        // Now P2 should get created
        fp2.get(1, TimeUnit.SECONDS);
    }

    @Test(dataProvider = "topics")
    public void exclusiveWithConsumers(String type, boolean partitioned) throws Exception {
        String topic = newTopic(type, partitioned);

        pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test")
                .subscribe();

        simpleTest(topic);
    }

    private String newTopic(String type, boolean isPartitioned) throws Exception {
        String topic = type + "://" + newTopicName();
        if (isPartitioned) {
            admin.topics().createPartitionedTopic(topic, 3);
        }

        return topic;
    }
}
