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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DriverImdgTest {

    private static final String JDBC_HAZELCAST_LOCALHOST = "jdbc:hazelcast://localhost:5701/public";

    private final Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);

    public DriverImdgTest() throws SQLException {
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
    public void shouldHazelcastJdbcConnection() {
        assertThat(connection).isNotNull();
    }

    @Test
    public void shouldExecuteSimpleQuery() throws SQLException {
        Statement statement = connection.createStatement();
        assertThat(statement).isNotNull();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person");
        List<Person> actualResult = new ArrayList<>();
        while (resultSet.next()) {
            actualResult.add(Person.valueOf(resultSet));
        }

        assertThat(actualResult).containsExactlyInAnyOrder(
                new Person("Jack0", 0), new Person("Jack1", 1), new Person("Jack2", 2));
    }

    @Test
    public void shouldUnwrapResultSet() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person WHERE name='Jack1'");
        resultSet.next();

        assertThat(resultSet.isWrapperFor(JdbcResultSet.class)).isTrue();
        assertThat(resultSet.unwrap(JdbcResultSet.class)).isNotNull();
    }

    @Test
    public void shouldNotHaveTimeoutIfNotSetExplicitly() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person WHERE name='Jack1'");
        resultSet.next();
        assertThat(resultSet.getString("name")).isNotNull();
    }

    @Test
    public void shouldReturnResultSet() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person WHERE name='Jack1'");

        assertThat(resultSet).isSameAs(statement.getResultSet());
    }

    @Test
    public void shouldExecuteSimplePreparedStatement() throws SQLException {
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM person WHERE name=? AND age=?");
        statement.setString(1, "Jack1");
        statement.setInt(2, 1);
        ResultSet resultSet = statement.executeQuery();
        resultSet.next();

        assertThat(Person.valueOf(resultSet)).isEqualTo(new Person("Jack1", 1));
    }

    @Test
    public void shouldCloseStatement() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person WHERE name='Jack1'");
        resultSet.next();

        statement.close();

        assertThat(statement.isClosed()).isTrue();
        assertThat(resultSet.isClosed()).isTrue();
    }

    @Test
    void shouldSupportSchemaFromConnectionString() throws SQLException {
        assertThat(connection.getSchema()).isEqualTo("public");
    }

    @Test
    void shouldExecuteSql() throws SQLException {
        Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);
        Statement statement = connection.createStatement();
        assertThat(statement).isNotNull();
        statement.executeQuery("SELECT * FROM person");
        ResultSet resultSet = statement.getResultSet();
        List<Person> actualResult = new ArrayList<>();
        while (resultSet.next()) {
            actualResult.add(Person.valueOf(resultSet));
        }

        assertThat(actualResult).containsExactlyInAnyOrder(
                new Person("Jack0", 0), new Person("Jack1", 1), new Person("Jack2", 2));
    }

    @Test
    void shouldFetchColumnResultsByIndex() throws SQLException {
        Statement statement = connection.createStatement();

        statement.executeQuery("SELECT * FROM person");
        ResultSet resultSet = statement.getResultSet();

        List<Integer> ages = new ArrayList<>();
        List<String> names = new ArrayList<>();
        while (resultSet.next()) {
            ages.add(resultSet.getInt(2));
            names.add(resultSet.getString(3));
        }
        assertThat(ages).containsExactlyInAnyOrder(0, 1, 2);
        assertThat(names).containsExactlyInAnyOrder("Jack0", "Jack1", "Jack2");
    }

    @Test
    void shouldReturnResultAsSetPerMaxRows() throws SQLException {
        Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);
        Statement statement = connection.createStatement();
        statement.setFetchSize(1);
        statement.setMaxRows(2);
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person");
        List<Person> actualResult = new ArrayList<>();
        while (resultSet.next()) {
            actualResult.add(Person.valueOf(resultSet));
        }

        assertThat(actualResult).hasSize(2);
    }

    @Test
    void shouldFindColumnLabelByIndex() throws SQLException {
        Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person");
        resultSet.next();
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertThat(metaData.getColumnLabel(1)).isEqualTo("__key");
        assertThat(metaData.getColumnLabel(2)).isEqualTo("age");
        assertThat(metaData.getColumnLabel(3)).isEqualTo("name");
    }
}
