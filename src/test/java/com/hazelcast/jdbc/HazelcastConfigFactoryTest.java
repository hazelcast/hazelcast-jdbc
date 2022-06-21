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
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.GcpConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.security.UsernamePasswordCredentials;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class HazelcastConfigFactoryTest {

    private final HazelcastConfigFactory configFactory = new HazelcastConfigFactory();

    @Test
    void shouldParseClusterNameConfiguration() {
        ClientConfig clientConfig = configFactory.clientConfig(
                new JdbcUrl("jdbc:hazelcast://localhost:5701/?clusterName=my-cluster", null));
        assertThat(clientConfig).isEqualTo(ClientConfig.load()
                .setNetworkConfig(new ClientNetworkConfig().setAddresses(Collections.singletonList("localhost:5701")))
                .setClusterName("my-cluster"));
    }

    @Test
    void shouldParseDiscoveryToken() {
        ClientConfig clientConfig = configFactory.clientConfig(
                new JdbcUrl("jdbc:hazelcast://cluster-name/?discoveryToken=token-value-123", null));
        assertThat(clientConfig).isEqualTo(ClientConfig.load()
                .setProperty(ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN.getName(), "token-value-123")
                .setClusterName("cluster-name"));
    }

    @Test
    void shouldParseCredentials() {
        ClientConfig clientConfig = configFactory.clientConfig(
                new JdbcUrl("jdbc:hazelcast://localhost:5701/?user=admin&password=pass", null));
        UsernamePasswordCredentials credentials = (UsernamePasswordCredentials) clientConfig.getSecurityConfig()
                .getCredentialsIdentityConfig().getCredentials();
        assertThat(credentials.getName()).isEqualTo("admin");
        assertThat(credentials.getPassword()).isEqualTo("pass");
    }

    @Test
    void shouldParseSslConfigs() {
        ClientConfig clientConfig = configFactory.clientConfig(
                new JdbcUrl("jdbc:hazelcast://localhost:5701/?sslEnabled=true&trustStore=truststore" +
                        "&trustStorePassword=123abc", null));
        ClientConfig expectedConfig = ClientConfig.load()
                .setNetworkConfig(new ClientNetworkConfig()
                        .setAddresses(Collections.singletonList("localhost:5701"))
                        .setSSLConfig(new SSLConfig()
                                .setEnabled(true)
                                .setProperty("trustStorePassword", "123abc")
                                .setProperty("trustStore", "truststore")));

        assertThat(clientConfig).isEqualTo(expectedConfig);
    }

    @Test
    void shouldParseAwsConfigs() {
        ClientConfig clientConfig = configFactory.clientConfig(
                new JdbcUrl("jdbc:hazelcast://localhost:5701/?awsTagKey=tagkey&awsTagValue=tagValue" +
                        "&awsAccessKey=accessKey&awsSecretKey=secretKey&awsIamRole=ADMIN&awsRegion=us-west-2&awsHostHeader=ec2" +
                        "&awsSecurityGroupName=securityGroup&awsConnectionTimeoutSeconds=10&awsReadTimeoutSeconds=5" +
                        "&awsConnectionRetries=3&awsHzPort=5801-5808&awsUsePublicIp=true", null));

        ClientConfig expectedConfig = ClientConfig.load()
                .setNetworkConfig(new ClientNetworkConfig()
                        .setAddresses(Collections.singletonList("localhost:5701"))
                        .setAwsConfig(new AwsConfig().setEnabled(true)
                                .setUsePublicIp(true)
                                .setProperty("tag-key", "tagkey")
                                .setProperty("tag-value", "tagValue")
                                .setProperty("access-key", "accessKey")
                                .setProperty("secret-key", "secretKey")
                                .setProperty("iam-role", "ADMIN")
                                .setProperty("region", "us-west-2")
                                .setProperty("host-header", "ec2")
                                .setProperty("security-group-name", "securityGroup")
                                .setProperty("connection-timeout-seconds", "10")
                                .setProperty("read-timeout-seconds", "5")
                                .setProperty("connection-retries", "3")
                                .setProperty("hz-port", "5801-5808")));
        assertThat(clientConfig).isEqualTo(expectedConfig);
    }

    @Test
    void shouldParseGcpConfigs() {
        ClientConfig clientConfig = configFactory.clientConfig(
                new JdbcUrl("jdbc:hazelcast://localhost:5701/?gcpPrivateKeyPath=/home/name/service/account/key.json" +
                        "&gcpProjects=project-1,project-2&gcpRegion=us-east1&gcpHzPort=5701-5708" +
                        "&gcpLabel=application=hazelcast&gcpUsePublicIp=true", null));
        ClientConfig expectedConfig = ClientConfig.load()
                .setNetworkConfig(new ClientNetworkConfig()
                        .setAddresses(Collections.singletonList("localhost:5701"))
                        .setGcpConfig(new GcpConfig()
                                .setEnabled(true)
                                .setUsePublicIp(true)
                                .setProperty("use-public-ip", "true")
                                .setProperty("private-key-path", "/home/name/service/account/key.json")
                                .setProperty("projects", "project-1,project-2")
                                .setProperty("region", "us-east1")
                                .setProperty("label", "application=hazelcast")
                                .setProperty("hz-port", "5701-5708")));
        assertThat(clientConfig).isEqualTo(expectedConfig);
    }

    @Test
    void shouldParseGcpConfigsWithMultipleLabels() {
        ClientConfig clientConfig = configFactory.clientConfig(
                new JdbcUrl("jdbc:hazelcast://localhost:5701/?gcpPrivateKeyPath=/home/name/service/account/key.json" +
                        "&gcpProjects=project-1,project-2&gcpRegion=us-east1&gcpHzPort=5701-5708" +
                        "&gcpLabel=application=hazelcast,other=value&gcpUsePublicIp=true", null));
        ClientConfig expectedConfig = ClientConfig.load()
                .setNetworkConfig(new ClientNetworkConfig()
                        .setAddresses(Collections.singletonList("localhost:5701"))
                        .setGcpConfig(new GcpConfig()
                                .setEnabled(true)
                                .setUsePublicIp(true)
                                .setProperty("use-public-ip", "true")
                                .setProperty("private-key-path", "/home/name/service/account/key.json")
                                .setProperty("projects", "project-1,project-2")
                                .setProperty("region", "us-east1")
                                .setProperty("label", "application=hazelcast,other=value")
                                .setProperty("hz-port", "5701-5708")));
        assertThat(clientConfig).isEqualTo(expectedConfig);
    }

    @Test
    void shouldParseConfigsForMultipleMemberInstances() {
        ClientConfig clientConfig = configFactory.clientConfig(
                new JdbcUrl("jdbc:hazelcast://198.51.100.11:5701,203.0.113.42:5702/", null));
        assertThat(clientConfig).isEqualTo(ClientConfig.load()
                .setNetworkConfig(new ClientNetworkConfig().setAddresses(Arrays.asList("198.51.100.11:5701", "203.0.113.42:5702")))
                .setClusterName("dev"));
    }

    @Test
    void shouldDecodeUrlEncodedHost() {
        ClientConfig clientConfig = configFactory.clientConfig(new JdbcUrl("jdbc:hazelcast://loc%61lhost:5701/", null));
        assertThat(clientConfig).isEqualTo(ClientConfig.load()
                .setNetworkConfig(new ClientNetworkConfig().setAddresses(Collections.singletonList("localhost:5701")))
                .setClusterName("dev"));
    }

    @Test
    void shouldParseSmartRouting() {
        String localMember = "localhost:5701";
        String baseUrl = "jdbc:hazelcast://" + localMember + "/";
        
        ClientConfig clientConfigWithout = configFactory.clientConfig(
                new JdbcUrl(baseUrl, null));
        ClientConfig clientConfigTrue = configFactory.clientConfig(
                new JdbcUrl(baseUrl + "?smart-routing=true", null));
        ClientConfig clientConfigFalse = configFactory.clientConfig(
                new JdbcUrl(baseUrl + "?smart-routing=false", null));
        
        List<String> addresses = Collections.singletonList(localMember);
        
        ClientConfig expectedClientConfigWithout = ClientConfig.load()
                .setNetworkConfig(new ClientNetworkConfig()
                        .setAddresses(addresses));
        ClientConfig expectedClientConfigTrue = ClientConfig.load()
                .setNetworkConfig(new ClientNetworkConfig()
                        .setSmartRouting(true).setAddresses(addresses));
        ClientConfig expectedClientConfigFalse = ClientConfig.load()
                .setNetworkConfig(new ClientNetworkConfig()
                        .setSmartRouting(false).setAddresses(addresses));

        assertThat(clientConfigWithout)
        .as("clientConfigWithout")
        .isEqualTo(expectedClientConfigWithout);
        assertThat(clientConfigTrue)
        .as("clientConfigTrue")
        .isEqualTo(expectedClientConfigTrue);
        assertThat(clientConfigFalse)
        .as("clientConfigFalse")
        .isEqualTo(expectedClientConfigFalse);
        assertThatExceptionOfType(RuntimeException.class)
        .as("clientConfigOther")
        .isThrownBy(() -> {
            configFactory.clientConfig(
                    new JdbcUrl(baseUrl + "?smart-routing=other", null));
        });
    }

    @Test
    void shouldParseBinary() {
        String localMember = "localhost:5701";
        String baseUrl = "jdbc:hazelcast://" + localMember + "/";
        String propertyName = System.getProperty("user.name");
        
        JdbcUrl urlNone = new JdbcUrl(baseUrl, null);
        JdbcUrl urlTrue = new JdbcUrl(baseUrl + "?" + propertyName + "=true", null);
        JdbcUrl urlFalse = new JdbcUrl(baseUrl + "?" + propertyName + "=false", null);
        JdbcUrl urlOther = new JdbcUrl(baseUrl + "?" + propertyName + "=other", null);

        boolean resultNoneTrue = HazelcastConfigFactory.parseBoolean(urlNone, propertyName, true);
        boolean resultNoneFalse = HazelcastConfigFactory.parseBoolean(urlNone, propertyName, false);
        boolean resultTrue = HazelcastConfigFactory.parseBoolean(urlTrue, propertyName, false);
        boolean resultFalse = HazelcastConfigFactory.parseBoolean(urlFalse, propertyName, true);
        
        assertThat(resultNoneTrue).as("resultNoneTrue").isEqualTo(true);
        assertThat(resultNoneFalse).as("resultNoneFalse").isEqualTo(false);
        assertThat(resultTrue).as("resultTrue").isEqualTo(true);
        assertThat(resultFalse).as("resultFalse").isEqualTo(false);
        assertThatExceptionOfType(RuntimeException.class)
        .as("resultOther")
        .isThrownBy(() -> HazelcastConfigFactory.parseBoolean(urlOther, propertyName, true));
    }    
}
