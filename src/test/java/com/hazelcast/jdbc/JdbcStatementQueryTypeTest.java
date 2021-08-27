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
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcStatementQueryTypeTest {

    private static final String JDBC_HAZELCAST_LOCALHOST = "jdbc:hazelcast://localhost:5701/";

    Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);

    JdbcStatementQueryTypeTest() throws SQLException {
    }

    @BeforeAll
    public static void beforeClass() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
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
    void shouldFailForUpdateOnSqlQuery() throws SQLException {
        Statement statement = connection.createStatement();

        assertThatThrownBy(() -> statement.executeUpdate("SELECT * FROM person"))
                .isInstanceOf(SQLException.class)
                .hasMessage("The statement doesn't produce update count");
    }

}
