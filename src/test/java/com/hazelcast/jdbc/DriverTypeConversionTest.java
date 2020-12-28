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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class DriverTypeConversionTest {

    private static final String JDBC_HAZELCAST_LOCALHOST = "jdbc:hazelcast://localhost:5701/public";
    private static HazelcastInstance member;

    @BeforeAll
    public static void beforeClass() {
        member = Hazelcast.newHazelcastInstance();
    }

    @AfterAll
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @AfterEach
    void tearDown() {
        member.getMap("types").clear();
    }

    @ParameterizedTest(name = "{index}: Value {0} of type should be converted to {1}")
    @MethodSource("typeValues")
    void shouldConvertTypes(Object value, Object expectedValue, ThrowingFunction<ResultSet, ?> getFromResultSet) throws SQLException {
        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM types");
        resultSet.next();
        assertThat(getFromResultSet.apply(resultSet)).isEqualTo(expectedValue);
    }

    private static Stream<Arguments> typeValues() {
        return Stream.of(
                Arguments.of("20000", new BigDecimal(20000), (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getBigDecimal("value")),
                Arguments.of(600_000, new BigDecimal(600_000), (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getBigDecimal(2)),
                Arguments.of(15, "15", (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getString("value")),
                Arguments.of(60, "60", (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getString(2)),
                Arguments.of("True", true, (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getBoolean("value")),
                Arguments.of("true", true, (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getBoolean(2)),
                Arguments.of(42, 42.0, (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getDouble("value")),
                Arguments.of(24, 24.0, (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getDouble(2)),
                Arguments.of(13, 13f, (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getFloat("value")),
                Arguments.of(27, 27f, (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getFloat(2)),
                Arguments.of(1, (byte)1, (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getByte("value")),
                Arguments.of(1, (byte)1, (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getByte(2)),
                Arguments.of(5L, (short)5, (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getShort("value")),
                Arguments.of(5L, (short)5, (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getShort(2)),
                Arguments.of(123L, 123, (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getInt("value")),
                Arguments.of(123L, 123, (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getInt(2)),
                Arguments.of(123, 123L, (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getLong("value")),
                Arguments.of(123, 123L, (ThrowingFunction<ResultSet, ?>) (ResultSet rs) -> rs.getLong(2))
        );
    }

    @FunctionalInterface
    private interface ThrowingFunction<T, R> {
        R apply(T t) throws SQLException;
    }
}