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
package org.apache.pulsar.broker.stats;

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.Metric;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.parseMetrics;
import static org.mockito.Mockito.mock;
import com.google.common.collect.Multimap;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.PrometheusMetricsTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.EntryFilterSupport;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.EntryFilterTest;
import org.apache.pulsar.broker.service.plugin.EntryFilterWithClassLoader;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class SubscriptionStatsTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration conf = super.getDefaultConf();
        // wait for shutdown of the broker, this prevents flakiness which could be caused by metrics being
        // unregistered asynchronously. This impacts the execution of the next test method if this would be happening.
        conf.setBrokerShutdownTimeoutMs(5000L);
        return conf;
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testNonContiguousDeletedMessagesRanges() throws Exception {
        final String topicName = "persistent://my-property/my-ns/testNonContiguousDeletedMessagesRanges-"
                + UUID.randomUUID().toString();
        final String subName = "my-sub";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        final int messages = 100;
        for (int i = 0; i < messages; i++) {
            producer.send(String.valueOf(i).getBytes());
        }

        for (int i = 0; i < messages; i++) {
            Message<byte[]> received = consumer.receive();
            if (i != 50) {
                consumer.acknowledge(received);
            }
        }

        Awaitility.await().untilAsserted(() -> {
            TopicStats stats = admin.topics().getStats(topicName);
            Assert.assertEquals(stats.getNonContiguousDeletedMessagesRanges(), 1);
            Assert.assertEquals(stats.getSubscriptions().size(), 1);
            Assert.assertEquals(stats.getSubscriptions().get(subName).getNonContiguousDeletedMessagesRanges(), 1);
            Assert.assertTrue(stats.getNonContiguousDeletedMessagesRangesSerializedSize() > 0);
            Assert.assertTrue(stats.getSubscriptions().get(subName)
                    .getNonContiguousDeletedMessagesRangesSerializedSize() > 0);
        });
    }

    @DataProvider(name = "testSubscriptionMetrics")
    public Object[][] topicAndSubscription() {
        return new Object[][]{
                {"persistent://my-property/my-ns/testSubscriptionStats-" + UUID.randomUUID(), "my-sub1", true, true},
                {"non-persistent://my-property/my-ns/testSubscriptionStats-"
                        + UUID.randomUUID(), "my-sub2", true, true},
                {"persistent://my-property/my-ns/testSubscriptionStats-" + UUID.randomUUID(), "my-sub3", false, true},
                {"non-persistent://my-property/my-ns/testSubscriptionStats-"
                        + UUID.randomUUID(), "my-sub4", false, true},

                {"persistent://my-property/my-ns/testSubscriptionStats-" + UUID.randomUUID(), "my-sub1", true, false},
                {"non-persistent://my-property/my-ns/testSubscriptionStats-"
                        + UUID.randomUUID(), "my-sub2", true, false},
                {"persistent://my-property/my-ns/testSubscriptionStats-" + UUID.randomUUID(), "my-sub3", false, false},
                {"non-persistent://my-property/my-ns/testSubscriptionStats-"
                        + UUID.randomUUID(), "my-sub4", false, false},
        };
    }

    @Test
    public void testSubscriptionStatsDispatchThrottled() throws Exception {

        final String topic = "persistent://my-property/my-ns/testSubscriptionStatsDispatchThrottled-"
                + UUID.randomUUID();
        final String subName = "my-sub";

        // Create topic and set subscription level dispatch rate
        admin.topics().createNonPartitionedTopic(topic);
        admin.topicPolicies().setSubscriptionDispatchRate(topic, subName, DispatchRateImpl.builder()
                .dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(1024)
                .ratePeriodInSecond(1)
                .build());

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(false)
                .create();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName(subName)
                .subscribe();

        for (int i = 0; i < 100; i++) {
            producer.newMessage().value(UUID.randomUUID().toString()).send();
        }

        for (int i = 0; i < 100; i++) {
            Message<String> message = consumer.receive(100, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            consumer.acknowledge(message);
        }

        // Assert subscription stats
        TopicStats topicStats = admin.topics().getStats(topic);
        SubscriptionStats stats = topicStats.getSubscriptions().get(subName);
        Assert.assertNotNull(stats);
        Assert.assertTrue(stats.getDispatchThrottledMsgEventsBySubscriptionLimit() > 0);
        Assert.assertTrue(stats.getDispatchThrottledBytesEventsBySubscriptionLimit() > 0);
        Assert.assertEquals(stats.getDispatchThrottledMsgEventsByTopicLimit(), 0);
        Assert.assertEquals(stats.getDispatchThrottledBytesEventsByTopicLimit(), 0);
        Assert.assertEquals(stats.getDispatchThrottledMsgEventsByBrokerLimit(), 0);
        Assert.assertEquals(stats.getDispatchThrottledBytesEventsByBrokerLimit(), 0);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, output);
        String metricsStr = output.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        // Assert subscription metrics reason by subscription limit
        Collection<Metric> subscriptionDispatchThrottledMsgCountMetrics =
                metrics.get("pulsar_subscription_dispatch_throttled_msg_events");
        Assert.assertFalse(subscriptionDispatchThrottledMsgCountMetrics.isEmpty());
        double subscriptionDispatchThrottledMsgCount = subscriptionDispatchThrottledMsgCountMetrics.stream()
                .filter(m -> m.tags.get("subscription").equals(subName)
                        && m.tags.get("topic").equals(topic) && m.tags.get("reason").equals("subscription"))
                .mapToDouble(m-> m.value).sum();
        Assert.assertTrue(subscriptionDispatchThrottledMsgCount > 0);

        double subscriptionAllDispatchThrottledMsgCount = subscriptionDispatchThrottledMsgCountMetrics.stream()
                .filter(m -> m.tags.get("subscription").equals(subName) && m.tags.get("topic").equals(topic))
                .mapToDouble(m-> m.value).sum();
        Assert.assertEquals(subscriptionDispatchThrottledMsgCount, subscriptionAllDispatchThrottledMsgCount);

        Collection<Metric> subscriptionDispatchThrottledBytesCountMetrics =
                metrics.get("pulsar_subscription_dispatch_throttled_bytes_events");
        Assert.assertFalse(subscriptionDispatchThrottledBytesCountMetrics.isEmpty());
        double subscriptionDispatchThrottledBytesCount = subscriptionDispatchThrottledBytesCountMetrics.stream()
                .filter(m -> m.tags.get("subscription").equals(subName)
                        && m.tags.get("topic").equals(topic) && m.tags.get("reason").equals("subscription"))
                .mapToDouble(m-> m.value).sum();
        Assert.assertTrue(subscriptionDispatchThrottledBytesCount > 0);
        double subscriptionAllDispatchThrottledBytesCount = subscriptionDispatchThrottledBytesCountMetrics.stream()
                .filter(m -> m.tags.get("subscription").equals(subName) && m.tags.get("topic").equals(topic))
                .mapToDouble(m-> m.value).sum();
        Assert.assertEquals(subscriptionDispatchThrottledBytesCount, subscriptionAllDispatchThrottledBytesCount);
    }

    @Test(dataProvider = "testSubscriptionMetrics")
    public void testSubscriptionStats(final String topic, final String subName, boolean enableTopicStats,
                                      boolean setFilter) throws Exception {
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(false)
                .create();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName(subName)
                .subscribe();

        boolean isPersistent = pulsar.getBrokerService().getTopic(topic, false).get().get().isPersistent();
        Dispatcher dispatcher = pulsar.getBrokerService().getTopic(topic, false).get()
                .get().getSubscription(subName).getDispatcher();

        if (setFilter) {
            Field field = EntryFilterSupport.class.getDeclaredField("entryFilters");
            field.setAccessible(true);
            Field hasFilterField = EntryFilterSupport.class.getDeclaredField("hasFilter");
            hasFilterField.setAccessible(true);
            NarClassLoader narClassLoader = mock(NarClassLoader.class);
            EntryFilter filter1 = new EntryFilterTest();
            EntryFilterWithClassLoader loader1 =
                    spyWithClassAndConstructorArgs(EntryFilterWithClassLoader.class, filter1, narClassLoader, false);
            field.set(dispatcher, List.of(loader1));
            hasFilterField.set(dispatcher, true);
        }

        int rejectedCount = 100;
        int acceptCount = 100;
        int scheduleCount = 100;
        for (int i = 0; i < rejectedCount; i++) {
            producer.newMessage().property("REJECT", " ").value(UUID.randomUUID().toString()).send();
        }
        for (int i = 0; i < acceptCount; i++) {
            producer.newMessage().property("ACCEPT", " ").value(UUID.randomUUID().toString()).send();
        }
        for (int i = 0; i < scheduleCount; i++) {
            producer.newMessage().property("RESCHEDULE", " ").value(UUID.randomUUID().toString()).send();
        }

        for (int i = 0; i < acceptCount; i++) {
            Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            consumer.acknowledge(message);
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, enableTopicStats, false, false, output);
        String metricsStr = output.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        Collection<Metric> throughFilterMetrics =
                metrics.get("pulsar_subscription_filter_processed_msg_count");
        Collection<Metric> acceptedMetrics =
                metrics.get("pulsar_subscription_filter_accepted_msg_count");
        Collection<Metric> rejectedMetrics =
                metrics.get("pulsar_subscription_filter_rejected_msg_count");
        Collection<Metric> rescheduledMetrics =
                metrics.get("pulsar_subscription_filter_rescheduled_msg_count");

        if (enableTopicStats) {
            Assert.assertTrue(throughFilterMetrics.size() > 0);
            Assert.assertTrue(acceptedMetrics.size() > 0);
            Assert.assertTrue(rejectedMetrics.size() > 0);
            Assert.assertTrue(rescheduledMetrics.size() > 0);

            double throughFilter = throughFilterMetrics.stream()
                    .filter(m -> m.tags.get("subscription").equals(subName) && m.tags.get("topic").equals(topic))
                    .mapToDouble(m-> m.value).sum();
            double filterAccepted = acceptedMetrics.stream()
                    .filter(m -> m.tags.get("subscription").equals(subName) && m.tags.get("topic").equals(topic))
                    .mapToDouble(m-> m.value).sum();
            double filterRejected = rejectedMetrics.stream()
                    .filter(m -> m.tags.get("subscription").equals(subName) && m.tags.get("topic").equals(topic))
                    .mapToDouble(m-> m.value).sum();
            double filterRescheduled = rescheduledMetrics.stream()
                    .filter(m -> m.tags.get("subscription").equals(subName) && m.tags.get("topic").equals(topic))
                    .mapToDouble(m-> m.value).sum();

            if (setFilter) {
                Assert.assertEquals(filterAccepted, acceptCount);
                Assert.assertEquals(filterRejected, rejectedCount);
                // Only works on the test, if there are some markers,
                // the filterProcessCount will be not equal with rejectedCount + rescheduledCount + acceptCount
                Assert.assertEquals(throughFilter,
                        filterAccepted + filterRejected + filterRescheduled, 0.01 * throughFilter);
            } else {
                Assert.assertEquals(throughFilter, 0D);
                Assert.assertEquals(filterAccepted, 0D);
                Assert.assertEquals(filterRejected, 0D);
                Assert.assertEquals(filterRescheduled, 0D);
            }
        } else {
            Assert.assertEquals(throughFilterMetrics.size(), 0);
            Assert.assertEquals(acceptedMetrics.size(), 0);
            Assert.assertEquals(rejectedMetrics.size(), 0);
            Assert.assertEquals(rescheduledMetrics.size(), 0);
        }

        testSubscriptionStatsAdminApi(topic, subName, setFilter, acceptCount, rejectedCount);
    }

    private void testSubscriptionStatsAdminApi(String topic, String subName, boolean setFilter,
                                               int acceptCount, int rejectedCount) throws Exception {
        boolean persistent = TopicName.get(topic).isPersistent();
        TopicStats topicStats = admin.topics().getStats(topic);
        SubscriptionStats stats = topicStats.getSubscriptions().get(subName);
        Assert.assertNotNull(stats);

        if (setFilter) {
            Assert.assertEquals(stats.getFilterAcceptedMsgCount(), acceptCount);
            if (persistent) {
                Assert.assertEquals(stats.getFilterRejectedMsgCount(), rejectedCount);
                // Only works on the test, if there are some markers,
                // the filterProcessCount will be not equal with rejectedCount + rescheduledCount + acceptCount
                Assert.assertEquals(stats.getFilterProcessedMsgCount(),
                        stats.getFilterAcceptedMsgCount() + stats.getFilterRejectedMsgCount()
                                + stats.getFilterRescheduledMsgCount(),
                        0.01 * stats.getFilterProcessedMsgCount());
            }
        } else {
            Assert.assertEquals(stats.getFilterAcceptedMsgCount(), 0L);
            if (persistent) {
                Assert.assertEquals(stats.getFilterRejectedMsgCount(), 0L);
                Assert.assertEquals(stats.getFilterAcceptedMsgCount(), 0L);
                Assert.assertEquals(stats.getFilterRescheduledMsgCount(), 0L);
            }
        }
    }
}
