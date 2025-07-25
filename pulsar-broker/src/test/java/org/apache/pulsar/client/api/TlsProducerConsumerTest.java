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
package org.apache.pulsar.client.api;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class TlsProducerConsumerTest extends TlsProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(TlsProducerConsumerTest.class);

    /**
     * verifies that messages whose size is larger than 2^14 bytes (max size of single TLS chunk) can be
     * produced/consumed.
     *
     * @throws Exception
     */
    @Test(timeOut = 30000)
    public void testTlsLargeSizeMessage() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int messageSize = 16 * 1024 + 1;
        log.info("-- message size -- {}", messageSize);

        internalSetUpForClient(true, pulsar.getBrokerServiceUrlTls());
        internalSetUpForNamespace();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1")
                .create();
        for (int i = 0; i < 10; i++) {
            byte[] message = new byte[messageSize];
            Arrays.fill(message, (byte) i);
            producer.send(message);
        }

        Message<byte[]> msg = null;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            byte[] expected = new byte[messageSize];
            Arrays.fill(expected, (byte) i);
            Assert.assertEquals(expected, msg.getData());
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 30000)
    public void testTlsClientAuthOverBinaryProtocol() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int messageSize = 16 * 1024 + 1;
        log.info("-- message size -- {}", messageSize);
        internalSetUpForNamespace();

        // Test 1 - Using TLS on binary protocol without sending certs - expect failure
        internalSetUpForClient(false, pulsar.getBrokerServiceUrlTls());
        try {
            pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
            Assert.fail("Server should have failed the TLS handshake since client didn't .");
        } catch (Exception ex) {
            // OK
        }

        // Test 2 - Using TLS on binary protocol - sending certs
        internalSetUpForClient(true, pulsar.getBrokerServiceUrlTls());
        try {
            pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
        } catch (Exception ex) {
            Assert.fail("Should not fail since certs are sent.");
        }
    }

    @Test(timeOut = 30000)
    public void testTlsClientAuthOverHTTPProtocol() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int messageSize = 16 * 1024 + 1;
        log.info("-- message size -- {}", messageSize);
        internalSetUpForNamespace();

        // Test 1 - Using TLS on https without sending certs - expect failure
        internalSetUpForClient(false, pulsar.getWebServiceAddressTls());
        try {
            pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
            Assert.fail("Server should have failed the TLS handshake since client didn't .");
        } catch (Exception ex) {
            // OK
        }

        // Test 2 - Using TLS on https - sending certs
        internalSetUpForClient(true, pulsar.getWebServiceAddressTls());
        try {
            pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
        } catch (Exception ex) {
            Assert.fail("Should not fail since certs are sent.");
        }
    }

    @Test(timeOut = 60000)
    public void testTlsCertsFromDynamicStream() throws Exception {
        log.info("-- Starting {} test --", methodName);
        String topicName = "persistent://my-property/use/my-ns/my-topic1";
        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrlTls())
                .enableTls(true).allowTlsInsecureConnection(false)
                .operationTimeout(1000, TimeUnit.MILLISECONDS);
        AtomicInteger index = new AtomicInteger(0);

        ByteArrayInputStream certStream = createByteInputStream(getTlsFileForClient("admin.cert"));
        ByteArrayInputStream keyStream = createByteInputStream(getTlsFileForClient("admin.key-pk8"));
        ByteArrayInputStream trustStoreStream = createByteInputStream(CA_CERT_FILE_PATH);

        Supplier<ByteArrayInputStream> certProvider = () -> getStream(index, certStream);
        Supplier<ByteArrayInputStream> keyProvider = () -> getStream(index, keyStream);
        Supplier<ByteArrayInputStream> trustStoreProvider = () -> getStream(index, trustStoreStream);
        AuthenticationTls auth = new AuthenticationTls(certProvider, keyProvider, trustStoreProvider);
        clientBuilder.authentication(auth);
        @Cleanup
        PulsarClient pulsarClient = clientBuilder.build();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscribe();

        // unload the topic so, new connection will be made and read the cert streams again
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        topicRef.close(false);

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1")
                .createAsync().get(30, TimeUnit.SECONDS);
        for (int i = 0; i < 10; i++) {
            producer.send(("test" + i).getBytes());
        }

        Message<byte[]> msg = null;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String exepctedMsg = "test" + i;
            Assert.assertEquals(exepctedMsg.getBytes(), msg.getData());
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * It verifies that AuthenticationTls provides cert refresh functionality.
     *
     * <pre>
     *  a. Create Auth with invalid cert
     *  b. Consumer fails with invalid tls certs
     *  c. refresh cert in provider
     *  d. Consumer successfully gets created
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testTlsCertsFromDynamicStreamExpiredAndRenewCert() throws Exception {
        log.info("-- Starting {} test --", methodName);
        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrlTls())
                .enableTls(true).allowTlsInsecureConnection(false)
                .autoCertRefreshSeconds(1)
                .operationTimeout(1000, TimeUnit.MILLISECONDS);
        AtomicInteger certIndex = new AtomicInteger(1);
        AtomicInteger keyIndex = new AtomicInteger(0);
        AtomicInteger trustStoreIndex = new AtomicInteger(1);
        ByteArrayInputStream certStream = createByteInputStream(getTlsFileForClient("admin.cert"));
        ByteArrayInputStream keyStream = createByteInputStream(getTlsFileForClient("admin.key-pk8"));
        ByteArrayInputStream trustStoreStream = createByteInputStream(CA_CERT_FILE_PATH);
        Supplier<ByteArrayInputStream> certProvider = () -> getStream(certIndex, certStream,
                keyStream/* invalid cert file */);
        Supplier<ByteArrayInputStream> keyProvider = () -> getStream(keyIndex, keyStream);
        Supplier<ByteArrayInputStream> trustStoreProvider = () -> getStream(trustStoreIndex, trustStoreStream,
                keyStream/* invalid cert file */);
        AuthenticationTls auth = new AuthenticationTls(certProvider, keyProvider, trustStoreProvider);
        clientBuilder.authentication(auth);
        @Cleanup
        PulsarClient pulsarClient = clientBuilder.build();
        Consumer<byte[]> consumer = null;
        try {
            consumer = pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
            Assert.fail("should have failed due to invalid tls cert");
        } catch (PulsarClientException e) {
            // Ok..
        }
        sleepSeconds(2);
        certIndex.set(0);
        try {
            consumer = pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
            Assert.fail("should have failed due to invalid tls cert");
        } catch (PulsarClientException e) {
            // Ok..
        }
        sleepSeconds(2);
        trustStoreIndex.set(0);
        sleepSeconds(2);
        consumer = pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    private ByteArrayInputStream createByteInputStream(String filePath) throws IOException {
        try (InputStream inStream = new FileInputStream(filePath)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            IOUtils.copy(inStream, baos);
            return new ByteArrayInputStream(baos.toByteArray());
        }
    }

    private ByteArrayInputStream getStream(AtomicInteger index, ByteArrayInputStream... streams) {
        return streams[index.intValue()];
    }

    private final Authentication tlsAuth =
        new AuthenticationTls(getTlsFileForClient("admin.cert"), getTlsFileForClient("admin.key-pk8"));

    @DataProvider
    public Object[] tlsTransport() {
        Supplier<String> webServiceAddressTls = () -> pulsar.getWebServiceAddressTls();
        Supplier<String> brokerServiceUrlTls = () -> pulsar.getBrokerServiceUrlTls();

        return new Object[][]{
                // Set TLS transport directly.
                {webServiceAddressTls, null},
                {brokerServiceUrlTls, null},
                // Using TLS authentication data to set up TLS transport.
                {webServiceAddressTls, tlsAuth},
                {brokerServiceUrlTls, tlsAuth},
        };
    }

    @Test(dataProvider = "tlsTransport")
    public void testTlsTransport(Supplier<String> url, Authentication auth) throws Exception {
        final String topicName = "persistent://my-property/my-ns/my-topic-1";

        internalSetUpForNamespace();

        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(url.get())
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .allowTlsInsecureConnection(false)
                .enableTlsHostnameVerification(false)
                .authentication(auth);

        if (auth == null) {
            clientBuilder.tlsKeyFilePath(getTlsFileForClient("admin.key-pk8"))
                    .tlsCertificateFilePath(getTlsFileForClient("admin.cert"));
        }

        @Cleanup
        PulsarClient client = clientBuilder.build();

        @Cleanup
        Producer<byte[]> ignored = client.newProducer().topic(topicName).create();
    }

    @Test
    public void testTlsWithFakeAuthentication() throws Exception {
        Authentication authentication = spy(new Authentication() {
            @Override
            public String getAuthMethodName() {
                return "fake";
            }

            @Override
            public void configure(Map<String, String> authParams) {

            }

            @Override
            public void start() {

            }

            @Override
            public void close() {

            }

            @Override
            public AuthenticationDataProvider getAuthData(String brokerHostName) {
                return mock(AuthenticationDataProvider.class);
            }
        });

        @Cleanup
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsar().getWebServiceAddressTls())
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .allowTlsInsecureConnection(false)
                .enableTlsHostnameVerification(false)
                .tlsKeyFilePath(getTlsFileForClient("admin.key-pk8"))
                .tlsCertificateFilePath(getTlsFileForClient("admin.cert"))
                .authentication(authentication)
                .build();
        pulsarAdmin.tenants().getTenants();
        verify(authentication, never()).getAuthData();

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPulsar().getBrokerServiceUrlTls())
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .allowTlsInsecureConnection(false)
                .enableTlsHostnameVerification(false)
                .tlsKeyFilePath(getTlsFileForClient("admin.key-pk8"))
                .tlsCertificateFilePath(getTlsFileForClient("admin.cert"))
                .authentication(authentication).build();
        verify(authentication, never()).getAuthData();

        final String topicName = "persistent://my-property/my-ns/my-topic-1";
        internalSetUpForNamespace();
        @Cleanup
        Consumer<byte[]> ignoredConsumer =
                pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name").subscribe();
        verify(authentication, never()).getAuthData();
        @Cleanup
        Producer<byte[]> ignoredProducer = pulsarClient.newProducer().topic(topicName).create();
        verify(authentication, never()).getAuthData();
    }
}
