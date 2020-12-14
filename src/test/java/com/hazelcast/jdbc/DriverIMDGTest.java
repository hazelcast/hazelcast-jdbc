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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class DriverIMDGTest {

    private static final String JDBC_HAZELCAST_LOCALHOST = "jdbc:hazelcast://localhost:5701/public";

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
    public void shouldHazelcastJdbcConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);
        assertThat(connection).isNotNull();
    }

    @Test
    public void shouldExecuteSimpleQuery() throws SQLException {
        Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);
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
        Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person WHERE name='Jack1'");
        resultSet.next();

        assertThat(resultSet.isWrapperFor(JdbcResultSet.class)).isTrue();
        assertThat(resultSet.unwrap(JdbcResultSet.class)).isNotNull();
    }

    @Test
    public void shouldNotHaveTimeoutIfNotSetExplicitly() throws SQLException {
        Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person WHERE name='Jack1'");
        resultSet.next();
        assertThat(resultSet.getString("name")).isNotNull();
    }

    @Test
    public void shouldReturnResultSet() throws SQLException {
        Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person WHERE name='Jack1'");

        assertThat(resultSet).isSameAs(statement.getResultSet());
    }

    @Test
    public void shouldExecuteSimplePreparedStatement() throws SQLException {
        Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM person WHERE name=? AND age=?");
        statement.setString(1, "Jack1");
        statement.setInt(2, 1);
        ResultSet resultSet = statement.executeQuery();
        resultSet.next();

        assertThat(Person.valueOf(resultSet)).isEqualTo(new Person("Jack1", 1));
    }


    @Test
    public void shouldCloseStatement() throws SQLException {
        Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person WHERE name='Jack1'");
        resultSet.next();

        statement.close();

        assertThat(statement.isClosed()).isTrue();
        assertThat(resultSet.isClosed()).isTrue();
    }

    @Test
    void shouldSupportSchemaFromConnectionString() throws SQLException {
        Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);
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
}
