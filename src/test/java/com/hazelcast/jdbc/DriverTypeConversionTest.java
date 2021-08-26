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
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

class DriverTypeConversionTest {

    private static final String JDBC_HAZELCAST_LOCALHOST = "jdbc:hazelcast://localhost:5701/";
    private static HazelcastInstance member;
    private final Connection connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);

    DriverTypeConversionTest() throws SQLException {
    }

    @BeforeAll
    public static void beforeClass() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    @AfterEach
    void tearDown() {
        HazelcastClient.shutdownAll();
        member.getMap("types").clear();
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to String")
    @MethodSource("values")
    void shouldConvertToStringType(Object value) throws SQLException {
        String expectedValue = String.valueOf(value);

        ResultSet resultSet = getResultSet(value);
        assertThat(resultSet.getString("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getString(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to int")
    @MethodSource("values")
    void shouldConvertToInteger(Object value) throws SQLException {
        Integer expectedValue = tryParse(value, o -> Integer.valueOf(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getResultSet(value);
        assertThat(resultSet.getInt("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getInt(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to int")
    @MethodSource("values")
    void shouldFailIfConversionToIntIsNotPossible(Object value) throws SQLException {
        Integer expectedValue = tryParse(value, QueryDataType.INT);
        assumeThat(expectedValue).isNull();

        ResultSet resultSet = getResultSet(value);
        assertThatThrownBy(() -> resultSet.getInt("value"))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to INTEGER"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to INTEGER"));
        assertThatThrownBy(() -> resultSet.getInt(2))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to INTEGER"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to INTEGER"));
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to long")
    @MethodSource("values")
    void shouldConvertToLong(Object value) throws SQLException {
        Long expectedValue = tryParse(value, o -> Long.valueOf(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getResultSet(value);
        assertThat(resultSet.getLong("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getLong(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to long")
    @MethodSource("values")
    void shouldFailIfConversionToLongIsNotPossible(Object value) throws SQLException {
        Long expectedValue = tryParse(value, QueryDataType.BIGINT);
        assumeThat(expectedValue).isNull();

        ResultSet resultSet = getResultSet(value);
        assertThatThrownBy(() -> resultSet.getLong("value"))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to BIGINT"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to BIGINT"));
        assertThatThrownBy(() -> resultSet.getLong(2))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to BIGINT"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to BIGINT"));
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to boolean")
    @MethodSource("values")
    void shouldConvertToBoolean(Object value) throws SQLException {
        Boolean expectedValue = tryParseBoolean(value);
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getResultSet(value);
        assertThat(resultSet.getBoolean("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getBoolean(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to boolean")
    @MethodSource("values")
    void shouldFailIfConversionToBooleanIsNotPossible(Object value) throws SQLException {
        Boolean expectedValue = tryParseBoolean(value);
        assumeThat(expectedValue).isNull();

        ResultSet resultSet = getResultSet(value);
        assertThatThrownBy(() -> resultSet.getBoolean("value"))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to BOOLEAN"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to BOOLEAN"));
        assertThatThrownBy(() -> resultSet.getBoolean(2))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to BOOLEAN"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to BOOLEAN"));
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to byte")
    @MethodSource("values")
    void shouldConvertToByte(Object value) throws SQLException {
        Byte expectedValue = tryParse(value, o -> Byte.parseByte(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getResultSet(value);
        assertThat(resultSet.getByte("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getByte(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to byte")
    @MethodSource("values")
    void shouldFailIfConversionToByteIsNotPossible(Object value) throws SQLException {
        Byte expectedValue = tryParse(value, QueryDataType.TINYINT);
        assumeThat(expectedValue).isNull();

        ResultSet resultSet = getResultSet(value);
        assertThatThrownBy(() -> resultSet.getByte("value"))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to TINYINT"),
                        t -> assertThat(t).hasMessage("Numeric overflow while converting " + sqlColumnType(value.getClass()) + " to TINYINT"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to TINYINT"));
        assertThatThrownBy(() -> resultSet.getByte(2))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to TINYINT"),
                        t -> assertThat(t).hasMessage("Numeric overflow while converting " + sqlColumnType(value.getClass()) + " to TINYINT"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to TINYINT"));
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to short")
    @MethodSource("values")
    void shouldConvertToShort(Object value) throws SQLException {
        Short expectedValue = tryParse(value, o -> Short.parseShort(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getResultSet(value);
        assertThat(resultSet.getShort("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getShort(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to short")
    @MethodSource("values")
    void shouldFailIfConversionToShortIsNotPossible(Object value) throws SQLException {
        Short expectedValue = tryParse(value, QueryDataType.SMALLINT);
        assumeThat(expectedValue).isNull();

        ResultSet resultSet = getResultSet(value);
        assertThatThrownBy(() -> resultSet.getShort("value"))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to SMALLINT"),
                        t -> assertThat(t).hasMessage("Numeric overflow while converting " + sqlColumnType(value.getClass()) + " to SMALLINT"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to SMALLINT"));
        assertThatThrownBy(() -> resultSet.getShort(2))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to SMALLINT"),
                        t -> assertThat(t).hasMessage("Numeric overflow while converting " + sqlColumnType(value.getClass()) + " to SMALLINT"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to SMALLINT"));
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to float")
    @MethodSource("values")
    void shouldConvertToFloat(Object value) throws SQLException {
        Float expectedValue = tryParse(value, o -> Float.parseFloat(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getResultSet(value);
        assertThat(resultSet.getFloat("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getFloat(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to float")
    @MethodSource("values")
    void shouldFailIfConversionToFloatIsNotPossible(Object value) throws SQLException {
        Float expectedValue = tryParse(value, o -> Float.parseFloat(o.toString()));
        assumeThat(expectedValue).isNull();

        ResultSet resultSet = getResultSet(value);
        assertThatThrownBy(() -> resultSet.getFloat("value"))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to REAL"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to REAL"));
        assertThatThrownBy(() -> resultSet.getFloat(2))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to REAL"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to REAL"));
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to double")
    @MethodSource("values")
    void shouldConvertToDouble(Object value) throws SQLException {
        Double expectedValue = tryParse(value, o -> Double.parseDouble(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getResultSet(value);
        assertThat(resultSet.getDouble("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getDouble(2)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to double")
    @MethodSource("values")
    void shouldFailIfConversionToDoubleIsNotPossible(Object value) throws SQLException {
        Double expectedValue = tryParse(value, o -> Double.parseDouble(o.toString()));
        assumeThat(expectedValue).isNull();

        ResultSet resultSet = getResultSet(value);
        assertThatThrownBy(() -> resultSet.getFloat("value"))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to REAL"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to REAL"));
        assertThatThrownBy(() -> resultSet.getFloat(2))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to REAL"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to REAL"));
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to BigDecimal")
    @MethodSource("values")
    void shouldConvertToBigDecimal(Object value) throws SQLException {
        BigDecimal expectedValue = tryParse(value, o -> new BigDecimal(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getResultSet(value);
        assertThat(resultSet.getBigDecimal("value").stripTrailingZeros()).isEqualTo(expectedValue.stripTrailingZeros());
        assertThat(resultSet.getBigDecimal(2).stripTrailingZeros()).isEqualTo(expectedValue.stripTrailingZeros());
    }

    @ParameterizedTest(name = "{index}: Should fail when converting {0} to BigDecimal")
    @MethodSource("values")
    void shouldFailIfConversionToBigDecimalIsNotPossible(Object value) throws SQLException {
        BigDecimal expectedValue = tryParse(value, QueryDataType.DECIMAL);
        assumeThat(expectedValue).isNull();

        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

        ResultSet resultSet = getResultSet(value);
        assertThatThrownBy(() -> resultSet.getBigDecimal("value"))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to DECIMAL"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to DECIMAL"));
        assertThatThrownBy(() -> resultSet.getBigDecimal(2))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to DECIMAL"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to DECIMAL"));
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to Object of type String")
    @MethodSource("values")
    void shouldConvertToObjectOfTypeString(Object value) throws SQLException {
        ResultSet resultSet = getResultSet(value);
        assertThat(resultSet.getObject("value", String.class)).isEqualTo(value.toString());
        assertThat(resultSet.getObject(2, String.class)).isEqualTo(value.toString());
    }

    @ParameterizedTest(name = "{index}: Should convert {0} to Object of type Integer")
    @MethodSource("values")
    void shouldConvertToObjectOfTypeInteger(Object value) throws SQLException {
        Integer expectedValue = tryParse(value, o -> Integer.valueOf(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getResultSet(value);
        assertThat(resultSet.getObject("value", Integer.class)).isEqualTo(expectedValue);
        assertThat(resultSet.getObject(2, Integer.class)).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "{index}: Should fail if converting of {0} to Object of type Integer")
    @MethodSource("values")
    void shouldFailIfConversionToTypedObjectIsNotPossible(Object value) throws SQLException {
        Integer expectedValue = tryParse(value, QueryDataType.INT);
        assumeThat(expectedValue).isNull();

        ResultSet resultSet = getResultSet(value);
        assertThatThrownBy(() -> resultSet.getObject("value", Integer.class))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to INTEGER"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to INTEGER"));
        assertThatThrownBy(() -> resultSet.getObject(2, Integer.class))
                .isInstanceOf(SQLException.class)
                .satisfiesAnyOf(
                        t -> assertThat(t).hasMessage("Cannot parse " + sqlColumnType(value.getClass()) + " value to INTEGER"),
                        t -> assertThat(t).hasMessage("Cannot convert " + sqlColumnType(value.getClass()) + " to INTEGER"));
    }

    @Test
    void shouldConvertToTimestamp() throws SQLException {
        Instant instant = Instant.ofEpochMilli(1609315200000L);
        ResultSet resultSet = getResultSet(instant);

        Timestamp expectedValue = new Timestamp(1609315200000L);
        assertThat(resultSet.getTimestamp("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getTimestamp(2)).isEqualTo(expectedValue);
    }

    @Test
    void shouldConvertToTime() throws SQLException {
        LocalTime localTime = LocalTime.of(7, 33);
        ResultSet resultSet = getResultSet(localTime);

        Time expectedValue = Time.valueOf(localTime);
        assertThat(resultSet.getTime("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getTime(2)).isEqualTo(expectedValue);
    }

    @Test
    void shouldConvertToDate() throws SQLException {
        LocalDate localDate = LocalDate.of(1999, Month.DECEMBER, 31);
        ResultSet resultSet = getResultSet(localDate);

        Date expectedValue = Date.valueOf(localDate);
        assertThat(resultSet.getDate("value")).isEqualTo(expectedValue);
        assertThat(resultSet.getDate(2)).isEqualTo(expectedValue);
    }

    @Test
    void shouldConvertObjectAsTimestamp() throws SQLException {
        Instant instant = Instant.ofEpochMilli(1609315200000L);
        ResultSet resultSet = getResultSet(instant);

        Timestamp expectedValue = new Timestamp(1609315200000L);
        assertThat(resultSet.getObject("value", Timestamp.class)).isEqualTo(expectedValue);
        assertThat(resultSet.getObject(2, Timestamp.class)).isEqualTo(expectedValue);
    }

    @Test
    void shouldConvertObjectAsTime() throws SQLException {
        LocalTime localTime = LocalTime.of(7, 33);
        ResultSet resultSet = getResultSet(localTime);

        Time expectedValue = Time.valueOf(localTime);
        assertThat(resultSet.getObject("value", Time.class)).isEqualTo(expectedValue);
        assertThat(resultSet.getObject(2, Time.class)).isEqualTo(expectedValue);
    }

    @Test
    void shouldConvertObjectAsDate() throws SQLException {
        LocalDate localDate = LocalDate.of(1999, Month.DECEMBER, 31);
        ResultSet resultSet = getResultSet(localDate);

        Date expectedValue = Date.valueOf(localDate);
        assertThat(resultSet.getObject("value", Date.class)).isEqualTo(expectedValue);
        assertThat(resultSet.getObject(2, Date.class)).isEqualTo(expectedValue);
    }

    @Test
    void shouldFailWhenRequestedObjectTypeIsWrong() throws SQLException {
        ResultSet resultSet = getResultSet("I'm a string object");

        assertThatThrownBy(() -> resultSet.getObject("value", Integer.class))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot parse VARCHAR value to INTEGER");

        assertThatThrownBy(() -> resultSet.getObject(2, Short.class))
                .isInstanceOf(SQLException.class)
                .hasMessage("Cannot parse VARCHAR value to SMALLINT");
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldSearchForVarcharParameter(Object value) throws SQLException {
        String expectedValue = String.valueOf(value);

        ResultSet resultSet = getPreparedResultSet(
                new ValuesWrapper(expectedValue), value, "SELECT * FROM types WHERE string = ?", Types.VARCHAR);
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("string")).isEqualTo(expectedValue);
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldSearchForBooleanParameter(Object value) throws SQLException {
        Boolean expectedValue = tryParseBoolean(value);
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getPreparedResultSet(
                new ValuesWrapper(expectedValue), value, "SELECT * FROM types WHERE \"boolean\" = ?", Types.BOOLEAN);

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getBoolean("boolean")).isEqualTo(expectedValue);
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldSearchForIntegerParameter(Object value) throws SQLException {
        Integer expectedValue = tryParse(value, o -> Integer.valueOf(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getPreparedResultSet(
                new ValuesWrapper(expectedValue), value, "SELECT * FROM types WHERE \"integer\" = ?", Types.INTEGER);

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getInt("integer")).isEqualTo(expectedValue);
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldSearchForLongParameter(Object value) throws SQLException {
        Long expectedValue = tryParse(value, o -> Long.valueOf(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getPreparedResultSet(
                new ValuesWrapper(expectedValue), value, "SELECT * FROM types WHERE \"long\" = ?", Types.BIGINT);

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getLong("long")).isEqualTo(expectedValue);
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldSearchForDecimalParameter(Object value) throws SQLException {
        BigDecimal expectedValue = tryParse(value, o -> new BigDecimal(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getPreparedResultSet(
                new ValuesWrapper(expectedValue), value, "SELECT * FROM types WHERE \"bigDecimal\" = ?", Types.DECIMAL);

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getBigDecimal("bigDecimal")).isEqualTo(expectedValue);
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldSearchForRealParameter(Object value) throws SQLException {
        Float expectedValue = tryParse(value, o -> Float.valueOf(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getPreparedResultSet(
                new ValuesWrapper(expectedValue), value, "SELECT * FROM types WHERE \"real\" = ?", Types.REAL);

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getFloat("real")).isEqualTo(expectedValue);
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldSearchForTinyIntParameter(Object value) throws SQLException {
        Byte expectedValue = tryParse(value, o -> Byte.valueOf(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getPreparedResultSet(
                new ValuesWrapper(expectedValue), value, "SELECT * FROM types WHERE \"tinyInt\" = ?", Types.TINYINT);

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getByte("tinyInt")).isEqualTo(expectedValue);
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldSearchForSmallIntParameter(Object value) throws SQLException {
        Short expectedValue = tryParse(value, o -> Short.valueOf(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getPreparedResultSet(
                new ValuesWrapper(expectedValue), value, "SELECT * FROM types WHERE \"smallInt\" = ?", Types.SMALLINT);

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getShort("smallInt")).isEqualTo(expectedValue);
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldSearchForDoubleParameter(Object value) throws SQLException {
        Double expectedValue = tryParse(value, o -> Double.valueOf(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getPreparedResultSet(
                new ValuesWrapper(expectedValue), value, "SELECT * FROM types WHERE \"double\" = ?", Types.DOUBLE);

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getDouble("double")).isEqualTo(expectedValue);
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldSearchForFloatParameter(Object value) throws SQLException {
        Float expectedValue = tryParse(value, o -> Float.valueOf(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getPreparedResultSet(
                new ValuesWrapper(expectedValue), value, "SELECT * FROM types WHERE \"real\" = ?", Types.FLOAT);

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getFloat("real")).isEqualTo(expectedValue);
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldSearchForNumericParameter(Object value) throws SQLException {
        BigDecimal expectedValue = tryParse(value, o -> new BigDecimal(o.toString()));
        assumeThat(expectedValue).isNotNull();

        ResultSet resultSet = getPreparedResultSet(
                new ValuesWrapper(expectedValue), value, "SELECT * FROM types WHERE \"bigDecimal\" = ?", Types.NUMERIC);

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getBigDecimal("bigDecimal")).isEqualTo(expectedValue);
    }

    @Test
    void shouldSearchForCharParameter() throws SQLException {
        Character expectedValue = 'c';

        ResultSet resultSet = getPreparedResultSet(
                new ValuesWrapper(expectedValue), "c", "SELECT * FROM types WHERE \"character\" = ?", Types.CHAR);

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("character")).isEqualTo(expectedValue.toString());
    }

    @Test
    void shouldThrowExceptionWhenSqlTypeIsNotSupported() throws SQLException {
        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new ValuesWrapper("String value"));
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM types WHERE \"string\" = ?");
        assertThatThrownBy(() -> preparedStatement.setObject(1, "String value", 999))
                .isInstanceOf(SQLException.class)
                .hasMessage("Target SQL type 999 is not supported");
    }

    @ParameterizedTest
    @MethodSource("nullableValues")
    void shouldConvertNullValues(ThrowingFunction<ResultSet, ?> resultSetFunction, Object expectedNullValue) throws SQLException {
        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(null));

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM types");

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSetFunction.apply(resultSet)).isEqualTo(expectedNullValue);
    }

    @Test
    void shouldSearchForTimestampWithTimezoneParameter() throws SQLException {
        Instant value = Instant.now();
        IMap<Object, Object> types = member.getMap("timestamp");
        types.put(1, value);

        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM \"timestamp\" WHERE \"this\" = ?");
        preparedStatement.setObject(1, value, Types.TIMESTAMP_WITH_TIMEZONE);
        ResultSet resultSet = preparedStatement.executeQuery();
        assertThat(resultSet.next()).isTrue();
    }

    @Test
    void shouldSearchForTimestampParameter() throws SQLException {
        LocalDateTime value = LocalDateTime.now();
        IMap<Object, Object> types = member.getMap("types");
        types.put(1, value);

        assertThat(getTemporalPreparedResultSet(value, Types.TIMESTAMP).next()).isTrue();
        assertThat(getTemporalPreparedResultSet(OffsetDateTime.of(value, ZoneOffset.UTC), Types.TIMESTAMP).next()).isTrue();
        assertThat(getTemporalPreparedResultSet(ZonedDateTime.of(value, ZoneOffset.UTC), Types.TIMESTAMP).next()).isTrue();
    }

    @Test
    void shouldSearchForDateParameter() throws SQLException {
        LocalDate value = LocalDate.now();
        IMap<Object, Object> types = member.getMap("types");
        types.put(1, value);

        assertThat(getTemporalPreparedResultSet(value, Types.DATE).next()).isTrue();
        assertThat(getTemporalPreparedResultSet(
                OffsetDateTime.of(value, LocalTime.now(), ZoneOffset.UTC), Types.DATE).next()).isTrue();
    }

    @Test
    void shouldSearchForTimeParameter() throws SQLException {
        LocalTime value = LocalTime.now();
        IMap<Object, Object> types = member.getMap("types");
        types.put(1, value);

        assertThat(getTemporalPreparedResultSet(value, Types.TIME).next()).isTrue();
        assertThat(getTemporalPreparedResultSet(
                OffsetDateTime.of(LocalDate.now(), value, ZoneOffset.UTC), Types.TIME).next()).isTrue();
    }

    @Test
    void shouldSearchForComparableObjects() throws SQLException {
        Person person = new Person("John", 20);
        IMap<Object, Object> types = member.getMap("person");
        types.put(1, person);

        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM person WHERE \"this\" = ?");
        preparedStatement.setObject(1, new Person("John", 20), Types.JAVA_OBJECT);
        ResultSet resultSet = preparedStatement.executeQuery();
        assertThat(resultSet.next()).isTrue();
    }

    @Test
    void shouldSearchForNullValues() throws SQLException {
        Person person = new Person("Jack", 20);
        IMap<Object, Object> types = member.getMap("person");
        types.put(1, person);

        PreparedStatement preparedStatement = connection.prepareStatement("SELECT CAST(? AS OBJECT), name, age FROM person");
        preparedStatement.setNull(1, Types.NULL);
        ResultSet resultSet = preparedStatement.executeQuery();
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getObject(1)).isNull();
        assertThat(resultSet.getString(2)).isEqualTo("Jack");
        assertThat(resultSet.getInt(3)).isEqualTo(20);
    }

    private ResultSet getTemporalPreparedResultSet(Object value, int sqlTargetType) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM types WHERE \"this\" = ?");
        preparedStatement.setObject(1, value, sqlTargetType);
        return preparedStatement.executeQuery();
    }


    private ResultSet getPreparedResultSet(ValuesWrapper valuesWrapper, Object value, String sql, int targetSqlType)
            throws SQLException {
        IMap<Object, Object> types = member.getMap("types");
        types.put(1, valuesWrapper);

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setObject(1, value, targetSqlType);
        return preparedStatement.executeQuery();
    }

    private ResultSet getResultSet(Object value) throws SQLException {
        IMap<Object, Object> types = member.getMap("types");
        types.put(1, new TypesHolder(value));

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
                Arguments.of(OffsetDateTime.of(LocalDateTime.of(2018, 6, 29, 14, 50), ZoneOffset.UTC)),
                Arguments.of(LocalTime.of(7, 33)),
                Arguments.of(LocalDate.of(2012, 12, 12))
        );
    }

    private static Stream<Arguments> nullableValues() {
        return Stream.of(
                Arguments.of(fromRS(rs -> rs.getString(2)), null),
                Arguments.of(fromRS(rs -> rs.getInt(2)), 0),
                Arguments.of(fromRS(rs -> rs.getLong(2)), 0L),
                Arguments.of(fromRS(rs -> rs.getBoolean(2)), false),
                Arguments.of(fromRS(rs -> rs.getBigDecimal(2)), null),
                Arguments.of(fromRS(rs -> rs.getFloat(2)), 0.0f),
                Arguments.of(fromRS(rs -> rs.getDouble(2)), 0.0),
                Arguments.of(fromRS(rs -> rs.getByte(2)), (byte) 0),
                Arguments.of(fromRS(rs -> rs.getShort(2)), (short) 0),
                Arguments.of(fromRS(rs -> rs.getTime(2)), null),
                Arguments.of(fromRS(rs -> rs.getTimestamp(2)), null),
                Arguments.of(fromRS(rs -> rs.getDate(2)), null),
                Arguments.of(fromRS(rs -> rs.getObject(2)), null)

        );
    }

    private static ThrowingFunction<ResultSet, ?> fromRS(ThrowingFunction<ResultSet, ?> function) {
        return function;
    }

    @SuppressWarnings("unchecked")
    private static <T> T tryParse(Object object, QueryDataType queryDataType) {
        try {
            return (T) queryDataType.convert(object);
        } catch (Exception e) {
            return null;
        }
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

    private static String sqlColumnType(Class<?> clazz) {
        for (SqlColumnType value : SqlColumnType.values()) {
            if (value.getValueClass().equals(clazz)) {
                return value.toString();
            }
        }
        return null;
    }

    private interface ThrowingFunction<T, R> {
        R apply(T t) throws SQLException;
    }
}
