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

    HazelcastSqlClient(JdbcUrl url) {
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
}
