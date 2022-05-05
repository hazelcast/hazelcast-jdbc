/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jdbc;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ConnectionRetryConfig;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

class HazelcastConfigFactory {

    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 5_000;
    private static final Map<String, BiConsumer<ClientConfig, String>> CONFIGURATION_MAPPING;

    static {
        Map<String, BiConsumer<ClientConfig, String>> map = new HashMap<>();
        map.put("clusterName", ClientConfig::setClusterName);
        gcpConfigMapping(map);
        awsConfigMapping(map);
        azureConfigMapping(map);
        sslConfigMapping(map);
        k8sConfigMapping(map);
        CONFIGURATION_MAPPING = Collections.unmodifiableMap(map);
    }

    private static void azureConfigMapping(Map<String, BiConsumer<ClientConfig, String>> map) {
        map.put("azureInstanceMetadataAvailable", (c, p) -> azureConfig(c, "instance-metadata-available", p));
        map.put("azureClientId", (c, p) -> azureConfig(c, "client-id", p));
        map.put("azureClientSecret", (c, p) -> azureConfig(c, "client-secret", p));
        map.put("azureTenantId", (c, p) -> azureConfig(c, "tenant-id", p));
        map.put("azureSubscriptionId", (c, p) -> azureConfig(c, "subscription-id", p));
        map.put("azureResourceGroup", (c, p) -> azureConfig(c, "resource-group", p));
        map.put("azureScaleSet", (c, p) -> azureConfig(c, "scale-set", p));
        map.put("azureUsePublicIp", (c, p) -> c.getNetworkConfig().getAzureConfig().setEnabled(true)
                .setUsePublicIp(p.equalsIgnoreCase("true")));
    }

    private static void k8sConfigMapping(Map<String, BiConsumer<ClientConfig, String>> map) {
        map.put("k8sServiceDns", (c, p) -> k8sConfig(c, "service-dns", p));
        map.put("k8sServiceDnsTimeout", (c, p) -> k8sConfig(c, "service-dns-timeout", p));
        map.put("k8sNamespace", (c, p) -> k8sConfig(c, "namespace", p));
        map.put("k8sServiceName", (c, p) -> k8sConfig(c, "service-name", p));
        map.put("k8sServicePort", (c, p) -> k8sConfig(c, "service-port", p));
    }

    private static void sslConfigMapping(Map<String, BiConsumer<ClientConfig, String>> map) {
        map.put("sslEnabled", (c, p) -> sslConfig(c, (ssl) -> ssl.setEnabled(p.equalsIgnoreCase("true"))));
        map.put("trustStore", (c, p) -> sslConfig(c, "trustStore", p));
        map.put("trustStorePassword", (c, p) -> sslConfig(c, "trustStorePassword", p));
        map.put("protocol", (c, p) -> sslConfig(c, "protocol", p));
        map.put("trustCertCollectionFile", (c, p) -> sslConfig(c, "trustCertCollectionFile", p));
        map.put("keyFile", (c, p) -> sslConfig(c, "keyFile", p));
        map.put("keyCertChainFile", (c, p) -> sslConfig(c, "keyCertChainFile", p));
        map.put("factoryClassName", (c, p) -> sslConfig(c, (ssl) -> ssl.setFactoryClassName(p)));
    }

    private static void awsConfigMapping(Map<String, BiConsumer<ClientConfig, String>> map) {
        map.put("awsTagKey", (c, v) -> awsConfig(c, "tag-key", v));
        map.put("awsTagValue", (c, v) -> awsConfig(c, "tag-value", v));
        map.put("awsAccessKey", (c, v) -> awsConfig(c, "access-key", v));
        map.put("awsSecretKey", (c, v) -> awsConfig(c, "secret-key", v));
        map.put("awsIamRole", (c, v) -> awsConfig(c, "iam-role", v));
        map.put("awsRegion", (c, v) -> awsConfig(c, "region", v));
        map.put("awsHostHeader", (c, v) -> awsConfig(c, "host-header", v));
        map.put("awsSecurityGroupName", (c, v) -> awsConfig(c, "security-group-name", v));
        map.put("awsConnectionTimeoutSeconds", (c, v) -> awsConfig(c, "connection-timeout-seconds", v));
        map.put("awsReadTimeoutSeconds", (c, v) -> awsConfig(c, "read-timeout-seconds", v));
        map.put("awsConnectionRetries", (c, v) -> awsConfig(c, "connection-retries", v));
        map.put("awsHzPort", (c, v) -> awsConfig(c, "hz-port", v));
        map.put("awsUsePublicIp", (c, v) -> awsConfig(c, "use-public-ip", v));
    }

    private static void gcpConfigMapping(Map<String, BiConsumer<ClientConfig, String>> map) {
        map.put("gcpPrivateKeyPath", (c, v) -> gcpConfig(c, "private-key-path", v));
        map.put("gcpHzPort", (c, v) -> gcpConfig(c, "hz-port", v));
        map.put("gcpProjects", (c, v) -> gcpConfig(c, "projects", v));
        map.put("gcpRegion", (c, v) -> gcpConfig(c, "region", v));
        map.put("gcpLabel", (c, v) -> gcpConfig(c, "label", v));
        map.put("gcpUsePublicIp", (c, v) -> gcpConfig(c, "use-public-ip", v));
    }

    ClientConfig clientConfig(JdbcUrl url) {
        ClientConfig clientConfig = securityConfig(url, ClientConfig.load());
        String discoveryToken = url.getProperty("discoveryToken");
        if (discoveryToken != null) {
            return cloudConfig(url, clientConfig, discoveryToken);
        }
        ClientNetworkConfig networkConfig = new ClientNetworkConfig().setAddresses(url.getAuthorities());
        clientConfig.setNetworkConfig(networkConfig);

        // JDBC users don't expect the driver to retry forever, if the driver can't connect. If the
        // user didn't provide his own setting, we'll set a non-infinity value (infinity is the default in hz client).
        ConnectionRetryConfig connectionRetryConfig = clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig();
        if (connectionRetryConfig.getClusterConnectTimeoutMillis() < 0) {
            connectionRetryConfig.setClusterConnectTimeoutMillis(DEFAULT_CONNECT_TIMEOUT_MILLIS);
        }

        CONFIGURATION_MAPPING.forEach((k, v) -> {
            String property = url.getProperty(k);
            if (property != null) {
                v.accept(clientConfig, property);
            }
        });
        return clientConfig;
    }

    private ClientConfig securityConfig(JdbcUrl url, ClientConfig clientConfig) {
        String user = url.getProperty("user");
        String password = url.getProperty("password");
        if (user != null || password != null) {
            clientConfig.getSecurityConfig().setCredentials(new UsernamePasswordCredentials(user, password));
        }
        return clientConfig;
    }

    private ClientConfig cloudConfig(JdbcUrl url, ClientConfig clientConfig, String discoveryToken) {
        clientConfig.setProperty(ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN.getName(), discoveryToken);
        clientConfig.setClusterName(url.getRawAuthority());
        return clientConfig;
    }

    private static void k8sConfig(ClientConfig clientConfig, String property, String value) {
        clientConfig.getNetworkConfig().getKubernetesConfig()
                .setEnabled(true)
                .setProperty(property, value);
    }

    private static void sslConfig(ClientConfig clientConfig, String property, String value) {
        SSLConfig sslConfig = clientConfig.getNetworkConfig().getSSLConfig();
        if (sslConfig == null) {
            sslConfig = new SSLConfig();
        }
        sslConfig.setProperty(property, value);
        clientConfig.getNetworkConfig().setSSLConfig(sslConfig);
    }

    private static void sslConfig(ClientConfig clientConfig, Consumer<SSLConfig> sslConfigFunction) {
        SSLConfig sslConfig = clientConfig.getNetworkConfig().getSSLConfig();
        if (sslConfig == null) {
            sslConfig = new SSLConfig();
        }
        sslConfigFunction.accept(sslConfig);
        clientConfig.getNetworkConfig().setSSLConfig(sslConfig);
    }

    private static void awsConfig(ClientConfig clientConfig, String property, String value) {
        clientConfig.getNetworkConfig()
                .getAwsConfig()
                .setEnabled(true)
                .setProperty(property, value);
    }

    private static void azureConfig(ClientConfig clientConfig, String property, String value) {
        clientConfig.getNetworkConfig()
                .getAzureConfig()
                .setEnabled(true)
                .setProperty(property, value);
    }

    private static void gcpConfig(ClientConfig clientConfig, String property, String value) {
        clientConfig.getNetworkConfig()
                .getGcpConfig()
                .setEnabled(true)
                .setProperty(property, value);
    }
}
