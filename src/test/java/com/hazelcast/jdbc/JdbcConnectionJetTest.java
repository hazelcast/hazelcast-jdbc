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
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class JdbcConnectionJetTest {

    @BeforeAll
    static void beforeAll() {
        JetInstance jetInstance = Jet.newJetInstance();
        IMap<Object, Object> person = jetInstance.getMap("person");
        person.put(1, new Person("Map", 1));

        SqlService sql = jetInstance.getSql();
        sql.execute("CREATE OR REPLACE MAPPING person(__key INT, name VARCHAR, age INT) TYPE IMap " +
                "OPTIONS('keyFormat'='int', 'valueFormat'='json')");
        sql.execute("SINK INTO person VALUES(2, 'Mapping', 2)");
    }

    @AfterAll
    static void afterAll() {
        HazelcastClient.shutdownAll();
        Jet.shutdownAll();
    }

    @Test
    void shouldNotAffectCreatedStatementsWhenChangingSchema() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:hazelcast://localhost:5701/public?clusterName=jet");
        Statement statement = connection.createStatement();
        connection.setSchema("partitioned");
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person");
        assertThat(resultSet.next()).isTrue();
        assertThat(Person.valueOf(resultSet)).isEqualTo(new Person("Map", 1));

        statement = connection.createStatement();
        resultSet = statement.executeQuery("SELECT * FROM person");
        assertThat(resultSet.next()).isTrue();
        assertThat(Person.valueOf(resultSet)).isEqualTo(new Person("Mapping", 1));
    }
}