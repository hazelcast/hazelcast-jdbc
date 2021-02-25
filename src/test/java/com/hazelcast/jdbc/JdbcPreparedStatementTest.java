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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JdbcPreparedStatementTest {


    private static final String JDBC_HAZELCAST_LOCALHOST = "jdbc:hazelcast://localhost:5701/public";
    private final Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);

    public JdbcPreparedStatementTest() throws SQLException {
    }

    @BeforeAll
    public static void setUp() {
        HazelcastInstance member = Hazelcast.newHazelcastInstance();
        IMap<Integer, Person> personMap = member.getMap("person");
        for (int i = 0; i < 3; i++) {
            personMap.put(i, new Person("Jack"+i, i));
        }
    }

    @AfterAll
    public static void tearDown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    void shouldFailOnAddingMoreParametersThanAllowed() throws SQLException {
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM person WHERE name=?");
        statement.setString(1, "Sam");
        statement.setString(2, "John");
        assertThatThrownBy(statement::execute)
                .isInstanceOf(SQLException.class)
                .hasMessage("Unexpected parameter count: expected 1, got 2");
    }

    @Test
    void shouldFailIfNotAllParametersWhereSet() throws SQLException {
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM person WHERE name=? AND age=?");
        statement.setInt(2, 27);
        assertThatThrownBy(statement::execute)
                .isInstanceOf(SQLException.class)
                .hasMessage("Parameter #1 is not set");
    }

    @Test
    void shouldFailOfNegativeParameterIndex() throws SQLException {
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM person WHERE name=?");
        assertThatThrownBy(() -> statement.setString(-1, "John"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Parameter index should be greater than zero");
    }
}