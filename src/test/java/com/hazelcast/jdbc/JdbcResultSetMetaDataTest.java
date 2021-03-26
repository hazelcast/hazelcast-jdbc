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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcResultSetMetaDataTest {

    private static final String JDBC_HAZELCAST_LOCALHOST = "jdbc:hazelcast://localhost:5701/";

    private final Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);

    public JdbcResultSetMetaDataTest() throws SQLException {
    }

    @BeforeAll
    public static void beforeClass() {
        HazelcastInstance member = Hazelcast.newHazelcastInstance();
        IMap<Integer, Person> personMap = member.getMap("person");
        for (int i = 0; i < 3; i++) {
            personMap.put(i, new Person("Jack" + i, i));
        }
    }

    @AfterAll
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    void shouldFindCorrectSignedColumns() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person");
        assertThat(resultSet.getMetaData().isSigned(2)).isTrue();
        assertThat(resultSet.getMetaData().isSigned(3)).isFalse();
    }

    @Test
    void shouldFindCorrectColumnDisplaySize() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person");
        assertThat(resultSet.getMetaData().getColumnDisplaySize(2)).isEqualTo(Constants.INTEGER_DISPLAY_SIZE);
        assertThat(resultSet.getMetaData().getColumnDisplaySize(3)).isEqualTo(Constants.STRING_DISPLAY_SIZE);
    }

    @Test
    void shouldFindColumnLabel() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person");
        assertThat(resultSet.getMetaData().getColumnLabel(2)).isEqualTo("age");
        assertThat(resultSet.getMetaData().getColumnLabel(3)).isEqualTo("name");
        assertThat(resultSet.getMetaData().getColumnName(2)).isEqualTo("age");
        assertThat(resultSet.getMetaData().getColumnName(3)).isEqualTo("name");
    }

    @Test
    void shouldFindColumnScaleAndPrecision() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person");
        assertThat(resultSet.getMetaData().getScale(2)).isEqualTo(Constants.ZERO);
        assertThat(resultSet.getMetaData().getScale(3)).isEqualTo(Constants.ZERO);
        assertThat(resultSet.getMetaData().getPrecision(2)).isEqualTo(Constants.INTEGER_DISPLAY_SIZE);
        assertThat(resultSet.getMetaData().getPrecision(3)).isEqualTo(Constants.MAX_STRING_LENGTH);
    }

    @Test
    void shouldFindColumnTypes() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person");
        assertThat(resultSet.getMetaData().getColumnType(2)).isEqualTo(Types.INTEGER);
        assertThat(resultSet.getMetaData().getColumnType(3)).isEqualTo(Types.VARCHAR);
        assertThat(resultSet.getMetaData().getColumnTypeName(2)).isEqualTo("INTEGER");
        assertThat(resultSet.getMetaData().getColumnTypeName(3)).isEqualTo("VARCHAR");
        assertThat(resultSet.getMetaData().getColumnClassName(2)).isEqualTo(Integer.class.getName());
        assertThat(resultSet.getMetaData().getColumnClassName(3)).isEqualTo(String.class.getName());
    }
}