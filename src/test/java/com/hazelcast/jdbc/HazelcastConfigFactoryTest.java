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
import com.hazelcast.config.SSLConfig;
import com.hazelcast.security.UsernamePasswordCredentials;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class HazelcastConfigFactoryTest {

    private final HazelcastConfigFactory configFactory = new HazelcastConfigFactory();

    @Test
    void shouldParseClusterNameConfiguration() {
        ClientConfig clientConfig = configFactory.clientConfig(
                JdbcUrl.valueOf("jdbc:hazelcast://localhost:5701/public?clusterName=my-cluster"));
        assertThat(clientConfig).isEqualTo(ClientConfig.load()
                .setNetworkConfig(new ClientNetworkConfig().setAddresses(Collections.singletonList("localhost:5701")))
                .setClusterName("my-cluster"));
    }

    @Test
    void shouldParseDiscoveryToken() {
        ClientConfig clientConfig = configFactory.clientConfig(
                JdbcUrl.valueOf("jdbc:hazelcast://cluster-name/public?discoverToken=token-value-123"));
        assertThat(clientConfig).isEqualTo(ClientConfig.load()
                .setProperty(ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN.getName(), "token-value-123")
                .setClusterName("cluster-name"));
    }

    @Test
    void shouldParseCredentials() {
        ClientConfig clientConfig = configFactory.clientConfig(
                JdbcUrl.valueOf("jdbc:hazelcast://localhost:5701/public?user=admin&password=pass"));
        UsernamePasswordCredentials credentials = (UsernamePasswordCredentials) clientConfig.getSecurityConfig()
                .getCredentialsIdentityConfig().getCredentials();
        assertThat(credentials.getName()).isEqualTo("admin");
        assertThat(credentials.getPassword()).isEqualTo("pass");
    }


    @Test
    void shouldParseSslConfigs() {
        ClientConfig clientConfig = configFactory.clientConfig(
                JdbcUrl.valueOf("jdbc:hazelcast://localhost:5701/public?sslEnabled=true&trustStore=truststore" +
                        "&trustStorePassword=123abc"));
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
                JdbcUrl.valueOf("jdbc:hazelcast://localhost:5701/public?awsTagKey=tagkey&awsTagValue=tagValue" +
                        "&awsAccessKey=accessKey&awsSecretKey=secretKey&awsIamRole=ADMIN&awsRegion=us-west-2&awsHostHeader=ec2" +
                        "&awsSecurityGroupName=securityGroup&awsConnectionTimeoutSeconds=10&awsReadTimeoutSeconds=5" +
                        "&awsConnectionRetries=3&awsHzPort=5801-5808&awsUsePublicIp=true"));

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
}