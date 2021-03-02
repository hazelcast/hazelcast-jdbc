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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class JdbcConnectionIntegrationTest {
    private HazelcastSqlClient client;

    @BeforeEach
    public void setUp() {
        HazelcastInstance member = Hazelcast.newHazelcastInstance();
        client = new HazelcastSqlClient(new JdbcUrl("jdbc:hazelcast://localhost:5701/public", null));

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
        assertThat(client.isRunning()).isFalse();
    }

    @Test
    void shouldNotSupportPrepareCall() {
        Connection connection = new JdbcConnection(client);
        assertThatThrownBy(() -> connection.prepareCall("{call getPerson(?, ?)}"))
                .isInstanceOf(SQLFeatureNotSupportedException.class)
                .hasMessage("CallableStatement not supported");
    }

    @Test
    void shouldAutoCloseStatementWhenResultSetIsClosed() throws SQLException {
        Connection connection = new JdbcConnection(client);
        Statement statement = connection.createStatement();
        statement.closeOnCompletion();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person");
        resultSet.close();

        assertThat(statement.isClosed()).isTrue();
    }
}
