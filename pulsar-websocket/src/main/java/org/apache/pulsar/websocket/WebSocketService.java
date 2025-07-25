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
package org.apache.pulsar.websocket;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.common.naming.Constants.WEBSOCKET_DUMMY_ORIGINAL_PRINCIPLE;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletException;
import javax.websocket.DeploymentException;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.internal.PropertiesUtils;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.apache.pulsar.websocket.stats.ProxyStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Socket proxy server which initializes other dependent services and starts server by opening web-socket end-point url.
 *
 */
public class WebSocketService implements Closeable {

    AuthenticationService authenticationService;
    AuthorizationService authorizationService;
    PulsarClient pulsarClient;

    private final ScheduledExecutorService executor;
    private PulsarResources pulsarResources;
    private MetadataStoreExtended configMetadataStore;
    private ServiceConfiguration config;

    @Getter
    private Optional<CryptoKeyReader> cryptoKeyReader = Optional.empty();

    private ClusterData localCluster;
    private final Map<String, Set<ProducerHandler>> topicProducerMap = new ConcurrentHashMap<>();
    private final Map<String, Set<ConsumerHandler>> topicConsumerMap = new ConcurrentHashMap<>();
    private final Map<String, Set<ReaderHandler>> topicReaderMap = new ConcurrentHashMap<>();
    private final ProxyStats proxyStats;

    public WebSocketService(WebSocketProxyConfiguration config) {
        this(createClusterData(config), PulsarConfigurationLoader.convertFrom(config));
    }

    public WebSocketService(ClusterData localCluster, ServiceConfiguration config) {
        this.config = config;
        this.executor = Executors
                .newScheduledThreadPool(config.getWebSocketNumServiceThreads(),
                        new DefaultThreadFactory("pulsar-websocket"));
        this.localCluster = localCluster;
        this.proxyStats = new ProxyStats(this);
    }

    public void start() throws PulsarServerException, PulsarClientException, MalformedURLException, ServletException,
            DeploymentException {

        if (isNotBlank(config.getConfigurationMetadataStoreUrl())) {
            try {
                configMetadataStore = createConfigMetadataStore(config.getConfigurationMetadataStoreUrl(),
                        (int) config.getMetadataStoreSessionTimeoutMillis(),
                        config.isMetadataStoreAllowReadOnlyOperations());
            } catch (MetadataStoreException e) {
                throw new PulsarServerException(e);
            }
            pulsarResources = new PulsarResources(null, configMetadataStore);
        }

        // start authorizationService
        if (config.isAuthorizationEnabled()) {
            if (pulsarResources == null) {
                throw new PulsarServerException(
                        "Failed to initialize authorization manager due to empty ConfigurationStoreServers");
            }
            authorizationService = new AuthorizationService(this.config, pulsarResources);
        }
        // start authentication service
        authenticationService = new AuthenticationService(this.config);
        // initialize crypto key reader
        String cryptoFactoryClassName = (String) config.getProperties().get("cryptoKeyReaderFactoryClassName");
        if (StringUtils.isNotBlank(cryptoFactoryClassName)) {
            try {
                CryptoKeyReaderFactory factoryInstance = (CryptoKeyReaderFactory) Class.forName(cryptoFactoryClassName)
                        .getDeclaredConstructor().newInstance();
                cryptoKeyReader = Optional.ofNullable(factoryInstance.create());
            } catch (Exception e) {
                log.info("Failed to initialize crypto-key reader", e);
                throw new PulsarServerException(e);
            }
        }

        log.info("Pulsar WebSocket Service started");
    }

    public MetadataStoreExtended createConfigMetadataStore(String serverUrls, int sessionTimeoutMs, boolean
            isAllowReadOnlyOperations)
            throws MetadataStoreException {
        return PulsarResources.createConfigMetadataStore(serverUrls, sessionTimeoutMs, isAllowReadOnlyOperations);
    }

    @Override
    public void close() throws IOException {
        if (pulsarClient != null) {
            pulsarClient.close();
        }

        if (authenticationService != null) {
            authenticationService.close();
        }

        if (configMetadataStore != null) {
            try {
                configMetadataStore.close();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        executor.shutdown();
    }

    public AuthenticationService getAuthenticationService() {
        return authenticationService;
    }

    public AuthorizationService getAuthorizationService() {
        return authorizationService;
    }

    public synchronized PulsarClient getPulsarClient() throws IOException {
        // Do lazy initialization of client
        if (pulsarClient == null) {
            if (localCluster == null) {
                // If not explicitly set, read clusters data from ZK
                localCluster = retrieveClusterData();
            }
            pulsarClient = createClientInstance(localCluster);
        }
        return pulsarClient;
    }

    public synchronized void setLocalCluster(ClusterData clusterData) {
        this.localCluster = clusterData;
    }

    private PulsarClient createClientInstance(ClusterData clusterData) throws IOException {
        ClientBuilder clientBuilder = PulsarClient.builder() //
                .memoryLimit(SizeUnit.MEGA_BYTES.toBytes(config.getWebSocketPulsarClientMemoryLimitInMB()),
                        SizeUnit.BYTES)
                .statsInterval(0, TimeUnit.SECONDS) //
                .enableTls(config.isTlsEnabled()) //
                .allowTlsInsecureConnection(config.isTlsAllowInsecureConnection()) //
                .enableTlsHostnameVerification(config.isTlsHostnameVerificationEnabled())
                .tlsTrustCertsFilePath(config.getBrokerClientTrustCertsFilePath()) //
                .ioThreads(config.getWebSocketNumIoThreads()) //
                .connectionsPerBroker(config.getWebSocketConnectionsPerBroker());
        if (clientBuilder instanceof  ClientBuilderImpl) {
            ((ClientBuilderImpl) clientBuilder).originalPrincipal(WEBSOCKET_DUMMY_ORIGINAL_PRINCIPLE);
        }

        // Apply all arbitrary configuration. This must be called before setting any fields annotated as
        // @Secret on the ClientConfigurationData object because of the way they are serialized.
        // See https://github.com/apache/pulsar/issues/8509 for more information.
        clientBuilder.loadConf(PropertiesUtils.filterAndMapProperties(config.getProperties(), "brokerClient_"));
        // Disabled auto release useless connection.
        clientBuilder.connectionMaxIdleSeconds(-1);

        if (isNotBlank(config.getBrokerClientAuthenticationPlugin())
                && isNotBlank(config.getBrokerClientAuthenticationParameters())) {
            clientBuilder.authentication(config.getBrokerClientAuthenticationPlugin(),
                    config.getBrokerClientAuthenticationParameters());
        }

        if (config.isBrokerClientTlsEnabled()) {
            if (isNotBlank(clusterData.getBrokerServiceUrlTls())) {
                clientBuilder.serviceUrl(clusterData.getBrokerServiceUrlTls());
            } else if (isNotBlank(clusterData.getServiceUrlTls())) {
                clientBuilder.serviceUrl(clusterData.getServiceUrlTls());
            }
        } else if (isNotBlank(clusterData.getBrokerServiceUrl())) {
            clientBuilder.serviceUrl(clusterData.getBrokerServiceUrl());
        } else {
            clientBuilder.serviceUrl(clusterData.getServiceUrl());
        }
        return clientBuilder.build();
    }

    private static ClusterData createClusterData(WebSocketProxyConfiguration config) {
        if (isNotBlank(config.getBrokerServiceUrl()) || isNotBlank(config.getBrokerServiceUrlTls())) {
            return ClusterData.builder()
                    .serviceUrl(config.getServiceUrl())
                    .serviceUrlTls(config.getServiceUrlTls())
                    .brokerServiceUrl(config.getBrokerServiceUrl())
                    .brokerServiceUrlTls(config.getBrokerServiceUrlTls())
                    .build();
        } else if (isNotBlank(config.getServiceUrl()) || isNotBlank(config.getServiceUrlTls())) {
            return ClusterData.builder()
                    .serviceUrl(config.getServiceUrl())
                    .serviceUrlTls(config.getServiceUrlTls())
                    .build();
        } else {
            return null;
        }
    }

    private ClusterData retrieveClusterData() throws PulsarServerException {
        if (pulsarResources == null) {
            throw new PulsarServerException(
                "Failed to retrieve Cluster data due to empty ConfigurationStoreServers");
        }
        try {
            return localCluster = pulsarResources.getClusterResources().getCluster(config.getClusterName())
                    .orElseThrow(() -> new NotFoundException("Cluster " + config.getClusterName()));
        } catch (Exception e) {
            throw new PulsarServerException(e);
        }
    }

    public ProxyStats getProxyStats() {
        return proxyStats;
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    public boolean isAuthenticationEnabled() {
        if (this.config == null) {
            return false;
        }
        return this.config.isAuthenticationEnabled();
    }

    public boolean isAuthorizationEnabled() {
        if (this.config == null) {
            return false;
        }
        return this.config.isAuthorizationEnabled();
    }

    public boolean addProducer(ProducerHandler producer) {
        return topicProducerMap
                .computeIfAbsent(producer.getProducer().getTopic(),
                        topic -> ConcurrentHashMap.newKeySet())
                .add(producer);
    }

    public Map<String, Set<ProducerHandler>> getProducers() {
        return topicProducerMap;
    }

    public boolean removeProducer(ProducerHandler producer) {
        final String topicName = producer.getProducer().getTopic();
        if (topicProducerMap.containsKey(topicName)) {
            return topicProducerMap.get(topicName).remove(producer);
        }
        return false;
    }

    public boolean addConsumer(ConsumerHandler consumer) {
        return topicConsumerMap
                .computeIfAbsent(consumer.getConsumer().getTopic(), topic -> ConcurrentHashMap.newKeySet())
                .add(consumer);
    }

    public Map<String, Set<ConsumerHandler>> getConsumers() {
        return topicConsumerMap;
    }

    public boolean removeConsumer(ConsumerHandler consumer) {
        final String topicName = consumer.getConsumer().getTopic();
        if (topicConsumerMap.containsKey(topicName)) {
            return topicConsumerMap.get(topicName).remove(consumer);
        }
        return false;
    }

    public boolean addReader(ReaderHandler reader) {
        return topicReaderMap.computeIfAbsent(reader.getConsumer().getTopic(), topic -> ConcurrentHashMap.newKeySet())
                .add(reader);
    }

    public Map<String, Set<ReaderHandler>> getReaders() {
        return topicReaderMap;
    }

    public boolean removeReader(ReaderHandler reader) {
        final String topicName = reader.getConsumer().getTopic();
        if (topicReaderMap.containsKey(topicName)) {
            return topicReaderMap.get(topicName).remove(reader);
        }
        return false;
    }

    public ServiceConfiguration getConfig() {
        return config;
    }

    private static final Logger log = LoggerFactory.getLogger(WebSocketService.class);
}
