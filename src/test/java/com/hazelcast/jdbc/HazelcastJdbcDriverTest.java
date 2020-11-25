package com.hazelcast.jdbc;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


public class HazelcastJdbcDriverTest {

    private static HazelcastInstance client;

    @BeforeClass
    public static void beforeClass() {
        HazelcastInstance member = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();

        MapConfig config = new MapConfig("person");
        member.getConfig().addMapConfig(config);
        IMap<Integer, Person> personMap = member.getMap("person");
        for (int i = 0; i < 3; i++) {
            personMap.put(i, new Person("Jack"+i, i));
        }
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void shouldHazelcastJdbcConnection() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:hazelcast://localhost:5701");
        assertThat(connection).isNotNull();
    }

    @Test
    public void shouldExecuteSimpleQuery() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:hazelcast://localhost:5701");
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
}
