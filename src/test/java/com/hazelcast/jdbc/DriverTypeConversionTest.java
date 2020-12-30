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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.Assumptions.assumeThatThrownBy;

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

    @ParameterizedTest(name = "{index}: Should convert {0} to String")
    @MethodSource("values")
    void shouldConvertToStringType(Object value) throws SQLException {
        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        String expectedValue = String.valueOf(value);

        ResultSet resultSet = getResultSet();
        assertThat(resultSet.getString("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getString(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to int")
    @MethodSource("values")
    void shouldConvertToInteger(Object value) throws SQLException {
        Integer expectedValue = tryParse(value, o -> Integer.valueOf(o.toString()));
        assumeThat(expectedValue).isNotNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assertThat(resultSet.getInt("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getInt(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to int")
    @MethodSource("values")
    void shouldFailIfConversionToIntIsNotPossible(Object value) throws SQLException {
        Integer expectedValue = tryParse(value, o -> Integer.valueOf(o.toString()));
        assumeThat(expectedValue).isNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assumeThatThrownBy(() -> resultSet.getInt("value"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to Integer");
        assumeThatThrownBy(() -> resultSet.getInt(2))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to Integer");
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to long")
    @MethodSource("values")
    void shouldConvertToLong(Object value) throws SQLException {
        Long expectedValue = tryParse(value, o -> Long.valueOf(o.toString()));
        assumeThat(expectedValue).isNotNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assertThat(resultSet.getLong("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getLong(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to long")
    @MethodSource("values")
    void shouldFailIfConversionToLongIsNotPossible(Object value) throws SQLException {
        Long expectedValue = tryParse(value, o -> Long.valueOf(o.toString()));
        assumeThat(expectedValue).isNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assumeThatThrownBy(() -> resultSet.getLong("value"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to Integer");
        assumeThatThrownBy(() -> resultSet.getLong(2))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to Integer");
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to boolean")
    @MethodSource("values")
    void shouldConvertToBoolean(Object value) throws SQLException {
        Boolean expectedValue = tryParseBoolean(value);
        assumeThat(expectedValue).isNotNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assertThat(resultSet.getBoolean("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getBoolean(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to boolean")
    @MethodSource("values")
    void shouldFailIfConversionToBooleanIsNotPossible(Object value) throws SQLException {
        Boolean expectedValue = tryParseBoolean(value);
        assumeThat(expectedValue).isNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assumeThatThrownBy(() -> resultSet.getBoolean("value"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to Integer");
        assumeThatThrownBy(() -> resultSet.getBoolean(2))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to Integer");
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to byte")
    @MethodSource("values")
    void shouldConvertToByte(Object value) throws SQLException {
        Byte expectedValue = tryParse(value, o -> Byte.parseByte(o.toString()));
        assumeThat(expectedValue).isNotNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assertThat(resultSet.getByte("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getByte(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to byte")
    @MethodSource("values")
    void shouldFailIfConversionToByteIsNotPossible(Object value) throws SQLException {
        Byte expectedValue = tryParse(value, o -> Byte.parseByte(o.toString()));
        assumeThat(expectedValue).isNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assumeThatThrownBy(() -> resultSet.getByte("value"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to Byte");
        assumeThatThrownBy(() -> resultSet.getByte(2))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to Byte");
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to short")
    @MethodSource("values")
    void shouldConvertToShort(Object value) throws SQLException {
        Short expectedValue = tryParse(value, o -> Short.parseShort(o.toString()));
        assumeThat(expectedValue).isNotNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assertThat(resultSet.getShort("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getShort(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to short")
    @MethodSource("values")
    void shouldFailIfConversionToShortIsNotPossible(Object value) throws SQLException {
        Short expectedValue = tryParse(value, o -> Short.parseShort(o.toString()));
        assumeThat(expectedValue).isNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assumeThatThrownBy(() -> resultSet.getShort("value"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to Short");
        assumeThatThrownBy(() -> resultSet.getShort(2))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to Short");
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to float")
    @MethodSource("values")
    void shouldConvertToFloat(Object value) throws SQLException {
        Float expectedValue = tryParse(value, o -> Float.parseFloat(o.toString()));
        assumeThat(expectedValue).isNotNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assertThat(resultSet.getFloat("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getFloat(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to float")
    @MethodSource("values")
    void shouldFailIfConversionToFloatIsNotPossible(Object value) throws SQLException {
        Float expectedValue = tryParse(value, o -> Float.parseFloat(o.toString()));
        assumeThat(expectedValue).isNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assumeThatThrownBy(() -> resultSet.getFloat("value"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to Float");
        assumeThatThrownBy(() -> resultSet.getFloat(2))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to Float");
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to double")
    @MethodSource("values")
    void shouldConvertToDouble(Object value) throws SQLException {
        Double expectedValue = tryParse(value, o -> Double.parseDouble(o.toString()));
        assumeThat(expectedValue).isNotNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assertThat(resultSet.getDouble("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getDouble(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to double")
    @MethodSource("values")
    void shouldFailIfConversionToDoubleIsNotPossible(Object value) throws SQLException {
        Double expectedValue = tryParse(value, o -> Double.parseDouble(o.toString()));
        assumeThat(expectedValue).isNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assumeThatThrownBy(() -> resultSet.getFloat("value"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to Double");
        assumeThatThrownBy(() -> resultSet.getFloat(2))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to Double");
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to BigDecimal")
    @MethodSource("values")
    void shouldConvertToBigDecimal(Object value) throws SQLException {
        BigDecimal expectedValue = tryParse(value, o -> new BigDecimal(o.toString()));
        assumeThat(expectedValue).isNotNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assertThat(resultSet.getBigDecimal("value").stripTrailingZeros()).isEqualTo(expectedValue.stripTrailingZeros());
        assertThat(resultSet.getBigDecimal(2).stripTrailingZeros()).isEqualTo(expectedValue.stripTrailingZeros());
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to BigDecimal")
    @MethodSource("values")
    void shouldFailIfConversionToBigDecimalIsNotPossible(Object value) throws SQLException {
        BigDecimal expectedValue = tryParse(value, o -> new BigDecimal(o.toString()));
        assumeThat(expectedValue).isNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet();
        assumeThatThrownBy(() -> resultSet.getBigDecimal("value"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to BigDecimal");
        assumeThatThrownBy(() -> resultSet.getBigDecimal(2))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot convert '" + value.toString() + "' of type " + value.getClass().getSimpleName() + " to BigDecimal");
    }

    private ResultSet getResultSet() throws SQLException {
        Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM types");
        resultSet.next();
        return resultSet;
    }

    private static Stream<Arguments> values() {
        return Stream.of(
                Arguments.of(new BigDecimal(600_000)),
                Arguments.of("Some string"),
                Arguments.of("60"),
                Arguments.of("True"),
                Arguments.of("false"),
                Arguments.of(true),
                Arguments.of(Boolean.FALSE),
                Arguments.of(42.0),
                Arguments.of(13f),
                Arguments.of((byte) 1),
                Arguments.of((short) 15),
                Arguments.of(123),
                Arguments.of(123L),
                Arguments.of(LocalDateTime.of(2018, 6, 29, 14, 50)),
                Arguments.of(ZonedDateTime.of(LocalDateTime.of(2018, 6, 29, 14, 50), ZoneOffset.UTC)),
                Arguments.of(LocalTime.of(7, 33)),
                Arguments.of(LocalDate.of(2012, 12, 12))
        );
    }

    private static <T> T tryParse(Object object, Function<Object, T> parsingFunction) {
        try {
            return parsingFunction.apply(object);
        } catch (Exception e) {
            return null;
        }
    }

    private static Boolean tryParseBoolean(Object object) {
        if (object.toString().equalsIgnoreCase("true")) {
            return true;
        } else if (object.toString().equalsIgnoreCase("false")) {
            return false;
        }
        return null;
    }
}