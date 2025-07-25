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
package org.apache.pulsar.proxy.server;

import static org.mockito.Mockito.spy;
import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class ProxyWithAuthorizationTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(ProxyWithAuthorizationTest.class);
    private static final String CLUSTER_NAME = "proxy-authorization";

    private static final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private static final String CLIENT_TOKEN = AuthTokenUtils.createToken(SECRET_KEY, "Client", Optional.empty());

    // The Proxy, Client, and SuperUser Client certs are signed by this CA
    private static final String TLS_TRUST_CERT_FILE_PATH = "./src/test/resources/authentication/tls/cacert.pem";

    // Proxy and Broker use valid certs that have no Subject Alternative Name to test hostname verification correctly
    // fails a connection to an invalid host.
    private static final String TLS_NO_SUBJECT_CERT_FILE_PATH =
            "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/no-subject-alt-cert.pem";
    private static final String TLS_NO_SUBJECT_KEY_FILE_PATH =
            "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/no-subject-alt-key.pem";
    private static final String TLS_PROXY_CERT_FILE_PATH =
            "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/proxy-cert.pem";
    private static final String TLS_PROXY_KEY_FILE_PATH =
            "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/proxy-key.pem";
    private static final String TLS_CLIENT_TRUST_CERT_FILE_PATH =
            "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/client-cacert.pem";
    private static final String TLS_CLIENT_CERT_FILE_PATH =
            "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/client-cert.pem";
    private static final String TLS_CLIENT_KEY_FILE_PATH =
            "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/client-key.pem";
    private static final String TLS_SUPERUSER_CLIENT_KEY_FILE_PATH =
            "./src/test/resources/authentication/tls/client-key.pem";
    private static final String TLS_SUPERUSER_CLIENT_CERT_FILE_PATH =
            "./src/test/resources/authentication/tls/client-cert.pem";

    private ProxyService proxyService;
    private WebServer webServer;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private Authentication proxyClientAuthentication;

    @DataProvider(name = "hostnameVerification")
    public Object[][] hostnameVerificationCodecProvider() {
        return new Object[][] {
            { Boolean.TRUE },
            { Boolean.FALSE }
        };
    }

    @DataProvider(name = "protocolsCiphersProvider")
    public Object[][] protocolsCiphersProviderCodecProvider() {
        // Test using defaults
        Set<String> ciphers1 = new TreeSet<>();
        Set<String> protocols1 = new TreeSet<>();

        // Test explicitly specifying protocols defaults
        Set<String> ciphers2 = new TreeSet<>();
        Set<String> protocols2 = new TreeSet<>();
        protocols2.add("TLSv1.3");
        protocols2.add("TLSv1.2");

        // Test for invalid ciphers
        Set<String> ciphers3 = new TreeSet<>();
        Set<String> protocols3 = new TreeSet<>();
        ciphers3.add("INVALID_PROTOCOL");

        // Incorrect Config since TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 was introduced in TLSv1.2
        Set<String> ciphers4 = new TreeSet<>();
        Set<String> protocols4 = new TreeSet<>();
        ciphers4.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        protocols4.add("TLSv1.1");

        // Incorrect Config since TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 was introduced in TLSv1.2
        Set<String> ciphers5 = new TreeSet<>();
        Set<String> protocols5 = new TreeSet<>();
        ciphers5.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        protocols5.add("TLSv1");

        // Correct Config
        Set<String> ciphers6 = new TreeSet<>();
        Set<String> protocols6 = new TreeSet<>();
        ciphers6.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        protocols6.add("TLSv1.2");

        // In correct config - JDK 8 doesn't support TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
        Set<String> ciphers7 = new TreeSet<>();
        Set<String> protocols7 = new TreeSet<>();
        protocols7.add("TLSv1.2");
        ciphers7.add("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");

        // Correct config - Atlease one of the Cipher Suite is supported
        Set<String> ciphers8 = new TreeSet<>();
        Set<String> protocols8 = new TreeSet<>();
        protocols8.add("TLSv1.2");
        ciphers8.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        ciphers8.add("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");

        return new Object[][] {
            { ciphers1, protocols1, Boolean.FALSE },
            { ciphers2, protocols2, Boolean.FALSE },
            { ciphers3, protocols3, Boolean.TRUE },
            { ciphers4, protocols4, Boolean.TRUE },
            { ciphers5, protocols5, Boolean.TRUE },
            { ciphers6, protocols6, Boolean.FALSE },
            { ciphers7, protocols7, Boolean.FALSE },
            { ciphers8, protocols8, Boolean.FALSE }
        };
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        // enable tls and auth&auth at broker
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setProxyRoles(Collections.singleton("Proxy"));
        conf.setAdvertisedAddress(null);

        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setBrokerServicePort(Optional.empty());
        conf.setWebServicePortTls(Optional.of(0));
        conf.setWebServicePort(Optional.empty());
        conf.setTlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);
        conf.setTlsCertificateFilePath(TLS_NO_SUBJECT_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(TLS_NO_SUBJECT_KEY_FILE_PATH);
        conf.setTlsAllowInsecureConnection(false);

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("superUser");
        superUserRoles.add("Proxy");
        conf.setSuperUserRoles(superUserRoles);

        conf.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        conf.setBrokerClientAuthenticationParameters(
                "tlsCertFile:" + TLS_SUPERUSER_CLIENT_CERT_FILE_PATH + "," + "tlsKeyFile:"
                        + TLS_SUPERUSER_CLIENT_KEY_FILE_PATH);
        conf.setBrokerClientTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);
        conf.setAuthenticationProviders(Set.of(AuthenticationProviderTls.class.getName(),
                AuthenticationProviderToken.class.getName()));
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(SECRET_KEY));
        conf.setProperties(properties);

        conf.setClusterName(CLUSTER_NAME);
        conf.setNumExecutorThreadPoolSize(5);
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.init();

        // start proxy service
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(false);
        proxyConfig.setForwardAuthorizationCredentials(true);
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setBrokerServiceURLTLS(pulsar.getBrokerServiceUrlTls());
        proxyConfig.setBrokerWebServiceURLTLS(pulsar.getWebServiceAddressTls());
        proxyConfig.setAdvertisedAddress(null);
        proxyConfig.setClusterName(CLUSTER_NAME);

        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setWebServicePortTls(Optional.of(0));
        proxyConfig.setTlsEnabledWithBroker(true);

        // enable tls and auth&auth at proxy
        proxyConfig.setTlsCertificateFilePath(TLS_NO_SUBJECT_CERT_FILE_PATH);
        proxyConfig.setTlsKeyFilePath(TLS_NO_SUBJECT_KEY_FILE_PATH);
        proxyConfig.setTlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);
        proxyConfig.setBrokerClientTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);
        proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(
                "tlsCertFile:" + TLS_PROXY_CERT_FILE_PATH + "," + "tlsKeyFile:" + TLS_PROXY_KEY_FILE_PATH);
        proxyConfig.setAuthenticationProviders(Set.of(AuthenticationProviderTls.class.getName(),
                AuthenticationProviderToken.class.getName()));
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(SECRET_KEY));
        proxyConfig.setProperties(properties);

        AuthenticationService authService =
                new AuthenticationService(PulsarConfigurationLoader.convertFrom(proxyConfig));
        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();
        proxyService = Mockito.spy(new ProxyService(proxyConfig, authService, proxyClientAuthentication));
        proxyService.setGracefulShutdown(false);
        webServer = new WebServer(proxyConfig, authService);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        proxyService.close();
        webServer.stop();
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
    }

    private void startProxy() throws Exception {
        proxyService.start();
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, proxyService, null, proxyClientAuthentication);
        webServer.start();
    }

    /**
     * <pre>
     * It verifies e2e tls + Authentication + Authorization (client -> proxy -> broker)
     *
     * 1. client connects to proxy over tls and pass auth-data
     * 2. proxy authenticate client and retrieve client-role
     *    and send it to broker as originalPrincipal over tls
     * 3. client creates producer/consumer via proxy
     * 4. broker authorize producer/consumer create request using originalPrincipal
     *
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testProxyAuthorization() throws Exception {
        log.info("-- Starting {} test --", methodName);

        startProxy();
        // Skip hostname verification because the certs intentionally do not have a hostname
        createProxyAdminClient(false);
        // create a client which connects to proxy over tls and pass authData
        @Cleanup
        PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrlTls(), PulsarClient.builder());

        String namespaceName = "my-tenant/my-ns";

        initializeCluster(admin, namespaceName);

        Consumer<byte[]> consumer = proxyClient.newConsumer()
                .topic("persistent://my-tenant/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();

        Producer<byte[]> producer = proxyClient.newProducer(Schema.BYTES)
                .topic("persistent://my-tenant/my-ns/my-topic1").create();
        final int msgs = 10;
        for (int i = 0; i < msgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = new HashSet<>();
        int count = 0;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            count++;
        }
        // Acknowledge the consumption of all messages at once
        Assert.assertEquals(msgs, count);
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "hostnameVerification")
    public void testTlsHostVerificationProxyToClient(boolean hostnameVerificationEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        startProxy();
        // Testing client to proxy hostname verification, so use the dataProvider's value here
        createProxyAdminClient(hostnameVerificationEnabled);
        // create a client which connects to proxy over tls and pass authData
        @Cleanup
        PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrlTls(),
                PulsarClient.builder().enableTlsHostnameVerification(hostnameVerificationEnabled));

        String namespaceName = "my-tenant/my-ns";

        try {
            initializeCluster(admin, namespaceName);
            if (hostnameVerificationEnabled) {
                Assert.fail("Connection should be failed due to hostnameVerification enabled");
            }
        } catch (PulsarAdminException e) {
            if (!hostnameVerificationEnabled) {
                Assert.fail("Cluster should initialize because hostnameverification is disabled");
            }
            admin.close();
            // Need new client because the admin client to proxy is failing due to hostname verification, and we still
            // want to test the binary protocol client fails to connect as well
            createProxyAdminClient(false);
            initializeCluster(admin, namespaceName);
        }

        try {
            proxyClient.newConsumer().topic("persistent://my-tenant/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
            if (hostnameVerificationEnabled) {
                Assert.fail("Connection should be failed due to hostnameVerification enabled");
            }
        } catch (PulsarClientException e) {
            if (!hostnameVerificationEnabled) {
                Assert.fail("Consumer should be created because hostnameverification is disabled");
            }
        }

        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * It verifies hostname verification at proxy when proxy tries to connect with broker. Proxy performs hostname
     * verification when broker sends its certs over tls .
     *
     * <pre>
     * 1. Broker sends certs back to proxy with CN="Broker" however, proxy tries to connect with hostname=localhost
     * 2. so, client fails to create consumer if proxy is enabled with hostname verification
     * </pre>
     *
     * @param hostnameVerificationEnabled
     * @throws Exception
     */
    @Test(dataProvider = "hostnameVerification")
    public void testTlsHostVerificationProxyToBroker(boolean hostnameVerificationEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        proxyConfig.setTlsHostnameVerificationEnabled(hostnameVerificationEnabled);
        startProxy();
        // This test skips hostname verification for client to proxy in order to test proxy to broker
        createProxyAdminClient(false);
        // create a client which connects to proxy over tls and pass authData
        @Cleanup
        PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrlTls(),
                PulsarClient.builder().operationTimeout(15, TimeUnit.SECONDS));

        String namespaceName = "my-tenant/my-ns";

        try {
            initializeCluster(admin, namespaceName);
            if (hostnameVerificationEnabled) {
                Assert.fail("Connection should be failed due to hostnameVerification enabled for proxy to broker");
            }
        } catch (PulsarAdminException.ServerSideErrorException e) {
            if (!hostnameVerificationEnabled) {
                Assert.fail("Cluster should initialize because hostnameverification is disabled for proxy to broker");
            }
            Assert.assertEquals(e.getStatusCode(), 502, "Should get bad gateway");
            admin.close();
            // Need to use broker's admin client because the proxy to broker is failing, and we still want to test
            // the binary protocol client fails to connect as well
            createBrokerAdminClient();
            initializeCluster(admin, namespaceName);
        }

        try {
            proxyClient.newConsumer().topic("persistent://my-tenant/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
            if (hostnameVerificationEnabled) {
                Assert.fail("Connection should be failed due to hostnameVerification enabled");
            }
        } catch (PulsarClientException e) {
            if (!hostnameVerificationEnabled) {
                Assert.fail("Consumer should be created because hostnameverification is disabled");
            }
        }

        log.info("-- Exiting {} test --", methodName);
        // reset
        proxyConfig.setTlsHostnameVerificationEnabled(false);
    }

    /*
     * This test verifies whether the Client and Proxy honor the protocols and ciphers specified. Details description of
     * test cases can be found in protocolsCiphersProviderCodecProvider
     */
    @Test(dataProvider = "protocolsCiphersProvider", timeOut = 5000)
    public void tlsCiphersAndProtocols(Set<String> tlsCiphers, Set<String> tlsProtocols, boolean expectFailure)
            throws Exception {
        log.info("-- Starting {} test --", methodName);
        String namespaceName = "my-tenant/my-ns";
        createBrokerAdminClient();

        initializeCluster(admin, namespaceName);

        ProxyConfiguration proxyConfig = new ProxyConfiguration();
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(false);
        proxyConfig.setForwardAuthorizationCredentials(true);
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setBrokerServiceURLTLS(pulsar.getBrokerServiceUrlTls());
        proxyConfig.setAdvertisedAddress(null);
        proxyConfig.setClusterName(CLUSTER_NAME);

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setWebServicePortTls(Optional.of(0));
        proxyConfig.setTlsEnabledWithBroker(true);

        // enable tls and auth&auth at proxy
        proxyConfig.setTlsCertificateFilePath(TLS_PROXY_CERT_FILE_PATH);
        proxyConfig.setTlsKeyFilePath(TLS_PROXY_KEY_FILE_PATH);
        proxyConfig.setTlsTrustCertsFilePath(TLS_CLIENT_TRUST_CERT_FILE_PATH);

        proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(
                "tlsCertFile:" + TLS_PROXY_CERT_FILE_PATH + "," + "tlsKeyFile:" + TLS_PROXY_KEY_FILE_PATH);
        proxyConfig.setBrokerClientTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderTls.class.getName());
        conf.setAuthenticationProviders(providers);
        proxyConfig.setAuthenticationProviders(providers);
        proxyConfig.setTlsProtocols(tlsProtocols);
        proxyConfig.setTlsCiphers(tlsCiphers);

        @Cleanup
        final Authentication proxyClientAuthentication =
                AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();

        @Cleanup
        ProxyService proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig)), proxyClientAuthentication));
        proxyService.setGracefulShutdown(false);
        try {
            proxyService.start();
        } catch (Exception ex) {
            if (!expectFailure) {
                Assert.fail("This test case should not fail");
            }
        }
        org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically((test) -> {
            try {
                return admin.namespaces().getPermissions(namespaceName).containsKey("Proxy")
                        && admin.namespaces().getPermissions(namespaceName).containsKey("Client");
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 3, 1000);
        try {
            @Cleanup
            PulsarClient proxyClient = createPulsarClient("pulsar://localhost:"
                    + proxyService.getListenPortTls().get(), PulsarClient.builder());
            Consumer<byte[]> consumer = proxyClient.newConsumer()
                    .topic("persistent://my-tenant/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();

            if (expectFailure) {
                Assert.fail("Failure expected for this test case");
            }
            consumer.close();
        } catch (Exception ex) {
            if (!expectFailure) {
                Assert.fail("This test case should not fail");
            }
        }
        admin.close();
        log.info("-- Exiting {} test --", methodName);
    }

    private final Authentication tlsAuth = new AuthenticationTls(TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH);
    private final Authentication tokenAuth = new AuthenticationToken(CLIENT_TOKEN);

    @DataProvider
    public Object[] tlsTransportWithAuth() {
        return new Object[]{
                tlsAuth,
                tokenAuth,
        };
    }

    @Test(dataProvider = "tlsTransportWithAuth")
    public void testProxyTlsTransportWithAuth(Authentication auth) throws Exception {
        log.info("-- Starting {} test --", methodName);

        startProxy();
        // Skip hostname verification because the certs intentionally do not have a hostname
        createProxyAdminClient(false);

        @Cleanup
        PulsarClient proxyClient = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrlTls())
                .statsInterval(0, TimeUnit.SECONDS)
                .authentication(auth)
                .tlsKeyFilePath(TLS_CLIENT_KEY_FILE_PATH)
                .tlsCertificateFilePath(TLS_CLIENT_CERT_FILE_PATH)
                .tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH)
                .operationTimeout(1000, TimeUnit.MILLISECONDS)
                .build();

        String namespaceName = "my-tenant/my-ns";

        initializeCluster(admin, namespaceName);

        Consumer<byte[]> consumer = proxyClient.newConsumer()
                .topic("persistent://my-tenant/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();

        Producer<byte[]> producer = proxyClient.newProducer(Schema.BYTES)
                .topic("persistent://my-tenant/my-ns/my-topic1").create();
        final int msgs = 10;
        for (int i = 0; i < msgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = new HashSet<>();
        int count = 0;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            count++;
        }
        // Acknowledge the consumption of all messages at once
        Assert.assertEquals(msgs, count);
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    private void initializeCluster(PulsarAdmin adminClient, String namespaceName) throws Exception {
        adminClient.clusters().createCluster("proxy-authorization", ClusterData.builder()
                .serviceUrlTls(brokerUrlTls.toString()).build());

        adminClient.tenants().createTenant("my-tenant",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("proxy-authorization")));
        adminClient.namespaces().createNamespace(namespaceName);

        adminClient.namespaces().grantPermissionOnNamespace(namespaceName, "Proxy",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        adminClient.namespaces().grantPermissionOnNamespace(namespaceName, "Client",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
    }

    private void createProxyAdminClient(boolean enableTlsHostnameVerification) throws Exception {
        Map<String, String> authParams = Maps.newHashMap();
        authParams.put("tlsCertFile", TLS_SUPERUSER_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_SUPERUSER_CLIENT_KEY_FILE_PATH);
        closeAdmin();
        admin = spy(PulsarAdmin.builder().serviceHttpUrl("https://localhost:" + webServer.getListenPortHTTPS().get())
                .tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH)
                .enableTlsHostnameVerification(enableTlsHostnameVerification)
                .authentication(AuthenticationTls.class.getName(), authParams).build());
    }

    private void createBrokerAdminClient() throws Exception {
        Map<String, String> authParams = Maps.newHashMap();
        authParams.put("tlsCertFile", TLS_SUPERUSER_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_SUPERUSER_CLIENT_KEY_FILE_PATH);
        closeAdmin();
        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrlTls.toString())
                .tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH)
                .authentication(AuthenticationTls.class.getName(), authParams).build());
    }

    @SuppressWarnings("deprecation")
    private PulsarClient createPulsarClient(String proxyServiceUrl, ClientBuilder clientBuilder)
            throws PulsarClientException {
        Map<String, String> authParams = Maps.newHashMap();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
        Authentication authTls = new AuthenticationTls();
        authTls.configure(authParams);

        return clientBuilder.serviceUrl(proxyServiceUrl).statsInterval(0, TimeUnit.SECONDS)
                .tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH)
                .authentication(authTls).enableTls(true)
                .operationTimeout(1000, TimeUnit.MILLISECONDS).build();
    }
}
