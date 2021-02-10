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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

class HazelcastConfigFactory {

    private static final Map<String, BiConsumer<ClientConfig, String>> CONFIGURATION_MAPPING;
    static {
        Map<String, BiConsumer<ClientConfig, String>> map = new HashMap<>();
        map.put("clusterName", ClientConfig::setClusterName);
        CONFIGURATION_MAPPING = Collections.unmodifiableMap(map);
    }

    ClientConfig clientConfig(JdbcUrl url) {
        ClientConfig clientConfig = ClientConfig.load();
        String discoverToken = url.getProperties().getProperty("discoverToken");
        if (discoverToken != null) {
            return hzCloudConfig(url, clientConfig, discoverToken);
        }
        ClientNetworkConfig networkConfig = new ClientNetworkConfig().addAddress(url.getAuthority());
        clientConfig.setNetworkConfig(networkConfig);
        CONFIGURATION_MAPPING.forEach((k, v) -> {
            String property = url.getProperties().getProperty(k);
            if (property != null) {
                v.accept(clientConfig, property);
            }
        });
        return clientConfig;
    }

    private ClientConfig hzCloudConfig(JdbcUrl url, ClientConfig clientConfig, String discoverToken) {
        clientConfig.setProperty(ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN.getName(), discoverToken);
        clientConfig.setClusterName(url.getAuthority());
        return clientConfig;
    }

}
