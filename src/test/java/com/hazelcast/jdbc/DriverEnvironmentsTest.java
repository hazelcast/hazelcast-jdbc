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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("envs")
class DriverEnvironmentsTest {

    private final String connectionString = System.getenv("CONNECTION_STRING");

    @BeforeEach
    void setUp() {
        HazelcastConfigFactory hazelcastConfigFactory = new HazelcastConfigFactory();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(hazelcastConfigFactory.clientConfig(JdbcUrl.valueOf(connectionString)));
        IMap<Integer, Person> people = client.getMap("person");
        people.put(1, new Person("Emma", 27));
        people.put(2, new Person("Olivia", 42));
        people.put(3, new Person("Sophia", 35));
    }

    @AfterEach
    void tearDown() {
        HazelcastClient.shutdownAll();
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "CONNECTION_STRING", matches = "\\S+")
    void shouldConnectToEnvironment() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person");

        List<Person> actualResult = new ArrayList<>();
        while (resultSet.next()) {
            actualResult.add(Person.valueOf(resultSet));
        }

        assertThat(actualResult).containsExactlyInAnyOrder(
                new Person("Emma", 27), new Person("Olivia", 42), new Person("Sophia", 35));
    }
}