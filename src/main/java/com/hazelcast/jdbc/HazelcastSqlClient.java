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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlStatement;

import java.util.Collections;

class HazelcastSqlClient {

    private final HazelcastInstance client;
    private final JdbcUrl jdbcUrl;

    HazelcastSqlClient(JdbcUrl url) {
        jdbcUrl = url;
        ClientNetworkConfig networkConfig = new ClientNetworkConfig().setAddresses(Collections.singletonList(url.getAuthority()));
        ClientConfig clientConfig = new ClientConfig().setNetworkConfig(networkConfig);
        String clusterName = url.getProperties().getProperty("clusterName");
        if (clusterName != null) {
            clientConfig.setClusterName(clusterName);
        }
        client = HazelcastClient.newHazelcastClient(clientConfig);
    }

    SqlResult execute(SqlStatement sqlStatement) {
        return client.getSql().execute(sqlStatement);
    }

    void shutdown() {
        client.shutdown();
    }

    boolean isRunning() {
        return client.getLifecycleService().isRunning();
    }

    HazelcastInstance getClient() {
        return client;
    }

    JdbcUrl getJdbcUrl() {
        return jdbcUrl;
    }
}
