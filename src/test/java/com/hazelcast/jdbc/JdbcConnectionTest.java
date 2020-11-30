package com.hazelcast.jdbc;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class JdbcConnectionTest {
    private HazelcastInstance client;

    @BeforeEach
    public void setUp() {
        HazelcastInstance member = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();

        MapConfig config = new MapConfig("person");
        member.getConfig().addMapConfig(config);
        IMap<Integer, Person> personMap = member.getMap("person");
        for (int i = 0; i < 3; i++) {
            personMap.put(i, new Person("Jack"+i, i));
        }
    }

    @AfterEach
    public void tearDown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void shouldCloseConnection() throws SQLException {
        Connection connection = new JdbcConnection(client);
        connection.close();
        assertThatThrownBy(connection::createStatement)
                .isInstanceOf(SQLException.class)
                .hasMessage("Connection is closed");
        assertThat(client.getLifecycleService().isRunning()).isFalse();
    }
}