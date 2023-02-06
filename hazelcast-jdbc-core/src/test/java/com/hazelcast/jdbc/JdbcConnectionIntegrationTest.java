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
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static com.hazelcast.jdbc.JdbcTestSupport.createMapping;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class JdbcConnectionIntegrationTest {
    private HazelcastInstance member;
    private HazelcastSqlClient client;

    @BeforeEach
    public void setUp() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        member = Hazelcast.newHazelcastInstance(config);

        client = new HazelcastSqlClient(new JdbcUrl("jdbc:hazelcast://localhost:5701/", null));
    }

    @AfterEach
    public void tearDown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void test_connectionClose() throws SQLException {
        Connection connection = new JdbcConnection(client);
        connection.close();
        assertThatThrownBy(connection::createStatement)
                .isInstanceOf(SQLException.class)
                .hasMessage("Connection is closed");
        assertThat(client.isRunning()).isFalse();
    }

    @Test
    void when_prepareCall_then_notSupported() {
        Connection connection = new JdbcConnection(client);
        assertThatThrownBy(() -> connection.prepareCall("{call getPerson(?, ?)}"))
                .isInstanceOf(SQLFeatureNotSupportedException.class)
                .hasMessage("CallableStatement not supported");
    }

    @Test
    void when_resultSetClosed_then_statementClosed() throws SQLException {
        Connection connection = new JdbcConnection(client);
        Statement statement = connection.createStatement();
        statement.closeOnCompletion();
        createMapping(member, "person", int.class, Person.class);
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person");
        resultSet.close();

        assertThat(statement.isClosed()).isTrue();
    }

    @Test
    void when_schemaChangedOnConnection_then_shouldNotAffectExistingStatements() throws SQLException {
        // test for https://github.com/hazelcast/hazelcast-jdbc/issues/58
        SqlService sql = member.getSql();
        sql.execute("CREATE OR REPLACE MAPPING mappings(__key INT, this INT) TYPE IMap "
                + "OPTIONS('keyFormat'='int', 'valueFormat'='int')");

        Connection connection = new JdbcConnection(client);
        Statement statement = connection.createStatement();
        connection.setSchema("information_schema");
        ResultSet resultSet = statement.executeQuery("SELECT * FROM mappings");
        Assertions.assertEquals("__key", resultSet.getMetaData().getColumnName(1));

        statement = connection.createStatement();
        resultSet = statement.executeQuery("SELECT * FROM mappings");
        Assertions.assertEquals("table_catalog", resultSet.getMetaData().getColumnName(1));
    }
}
