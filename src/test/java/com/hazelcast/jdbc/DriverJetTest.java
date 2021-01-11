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

import com.hazelcast.config.Config;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@Disabled
public class DriverJetTest {

    @BeforeAll
    static void beforeAll() {
        AtomicInteger i = new AtomicInteger(1);
        Pipeline pipeline = Pipeline.create();

        pipeline.readFrom(TestSources.items("Jack", "John", "Albert"))
                .map(name -> new Person(name, 22))
                .writeTo(Sinks.map("person", p -> i.getAndIncrement(), p -> p));

        JetConfig jetConfig = new JetConfig();
        jetConfig.setHazelcastConfig(new Config().setClusterName("my-cluster"));
        JetInstance jet = Jet.newJetInstance(jetConfig);
        jet.newJob(pipeline).join();
    }

    @AfterAll
    static void afterAll() {
        Jet.shutdownAll();
    }

    @Test
    void shouldQueryJet() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:hazelcast://localhost:5701/public?clusterName=my-cluster");
        Statement statement = connection.createStatement();
        assertThat(statement).isNotNull();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person");
        List<Person> actualResult = new ArrayList<>();
        while (resultSet.next()) {
            actualResult.add(Person.valueOf(resultSet));
        }

        assertThat(actualResult).containsExactlyInAnyOrder(
                new Person("Jack", 22), new Person("John", 22), new Person("Albert", 22));
    }
}