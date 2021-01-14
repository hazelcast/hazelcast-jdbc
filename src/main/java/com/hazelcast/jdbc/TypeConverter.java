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

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;
import com.hazelcast.sql.impl.type.converter.Converters;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

final class TypeConverter {

    private static final int MILLIS_IN_SECONDS = 1_000;
    private static final Map<Integer, QueryDataType> SQL_TYPES_TO_QUERY_DATA_TYPE = new HashMap<>();
    private static final Map<SqlColumnType, QueryDataType> SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP = new HashMap<>();

    static {
        SQL_TYPES_TO_QUERY_DATA_TYPE.put(Types.VARCHAR, QueryDataType.VARCHAR);
        SQL_TYPES_TO_QUERY_DATA_TYPE.put(Types.BOOLEAN, QueryDataType.BOOLEAN);
        SQL_TYPES_TO_QUERY_DATA_TYPE.put(Types.INTEGER, QueryDataType.INT);
        SQL_TYPES_TO_QUERY_DATA_TYPE.put(Types.BIGINT, QueryDataType.BIGINT);
        SQL_TYPES_TO_QUERY_DATA_TYPE.put(Types.DECIMAL, QueryDataType.DECIMAL);
        SQL_TYPES_TO_QUERY_DATA_TYPE.put(Types.REAL, QueryDataType.REAL);
        SQL_TYPES_TO_QUERY_DATA_TYPE.put(Types.TINYINT, QueryDataType.TINYINT);
        SQL_TYPES_TO_QUERY_DATA_TYPE.put(Types.SMALLINT, QueryDataType.SMALLINT);
        SQL_TYPES_TO_QUERY_DATA_TYPE.put(Types.DOUBLE, QueryDataType.DOUBLE);
        SQL_TYPES_TO_QUERY_DATA_TYPE.put(Types.FLOAT, QueryDataType.REAL);
        SQL_TYPES_TO_QUERY_DATA_TYPE.put(Types.NUMERIC, QueryDataType.DECIMAL);
        SQL_TYPES_TO_QUERY_DATA_TYPE.put(Types.CHAR, QueryDataType.VARCHAR_CHARACTER);

        SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.put(SqlColumnType.VARCHAR, QueryDataType.VARCHAR);
        SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.put(SqlColumnType.BOOLEAN, QueryDataType.BOOLEAN);
        SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.put(SqlColumnType.INTEGER, QueryDataType.INT);
        SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.put(SqlColumnType.BIGINT, QueryDataType.BIGINT);
        SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.put(SqlColumnType.DECIMAL, QueryDataType.DECIMAL);
        SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.put(SqlColumnType.REAL, QueryDataType.REAL);
        SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.put(SqlColumnType.TINYINT, QueryDataType.TINYINT);
        SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.put(SqlColumnType.SMALLINT, QueryDataType.SMALLINT);
        SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.put(SqlColumnType.DOUBLE, QueryDataType.DOUBLE);
        SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.put(SqlColumnType.OBJECT, QueryDataType.OBJECT);
    }

    private TypeConverter() {
    }

    @SuppressWarnings("unchecked")
    static <T> T convertTo(Object object, QueryDataType targetDataType) throws SQLException {
        try {
            return (T) targetDataType.convert(object);
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    static <T> T convertTo(Object object, Class<T> clazz) throws SQLException {
        if (clazz == Timestamp.class) {
            return (T) convertToTimestamp(object);
        }
        if (clazz == Time.class) {
            return (T) convertToTime(object);
        }
        if (clazz == Date.class) {
            return (T) convertToDate(object);
        }
        QueryDataType queryDataType = QueryDataTypeUtils.resolveTypeForClass(clazz);
        try {
            return (T) queryDataType.convert(object);
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    static <T> T convertTo(Object object, int targetSqlType) throws SQLException {
        QueryDataType queryDataType = SQL_TYPES_TO_QUERY_DATA_TYPE.get(targetSqlType);
        if (queryDataType == null) {
            throw new SQLException("Target SQL type " + targetSqlType + " is not supported");
        }
        return (T) queryDataType.convert(object);
    }

    static double convertToDouble(Object object, SqlColumnType columnType) throws SQLException {
        if (object == null) {
            return 0;
        }
        QueryDataType queryDataType = SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.get(columnType);
        try {
            return queryDataType.getConverter().asDouble(object);
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    static float convertToFloat(Object object, SqlColumnType columnType) throws SQLException {
        if (object == null) {
            return 0f;
        }
        QueryDataType queryDataType = SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.get(columnType);
        try {
            return queryDataType.getConverter().asReal(object);
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    static boolean convertToBoolean(Object object, SqlColumnType columnType) throws SQLException {
        if (object == null) {
            return false;
        }
        QueryDataType queryDataType = SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.get(columnType);
        try {
            return queryDataType.getConverter().asBoolean(object);
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    static byte convertToByte(Object object, SqlColumnType columnType) throws SQLException {
        if (object == null) {
            return 0;
        }
        QueryDataType queryDataType = SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.get(columnType);
        try {
            return queryDataType.getConverter().asTinyint(object);
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    static short convertToShort(Object object, SqlColumnType columnType) throws SQLException {
        if (object == null) {
            return 0;
        }
        QueryDataType queryDataType = SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.get(columnType);
        try {
            return queryDataType.getConverter().asSmallint(object);
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    static long convertToLong(Object object, SqlColumnType columnType) throws SQLException {
        if (object == null) {
            return 0;
        }
        QueryDataType queryDataType = SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.get(columnType);
        try {
            return queryDataType.getConverter().asBigint(object);
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    static int convertToInt(Object object, SqlColumnType columnType) throws SQLException {
        if (object == null) {
            return 0;
        }
        QueryDataType queryDataType = SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.get(columnType);
        try {
            return queryDataType.getConverter().asInt(object);
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    static String convertToString(Object object, SqlColumnType columnType) throws SQLException {
        if (object == null) {
            return null;
        }
        QueryDataType queryDataType = SQL_COLUMN_TYPE_TO_QUERY_DATA_TYPE_MAP.get(columnType);
        try {
            return queryDataType.getConverter().asVarchar(object);
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    static Timestamp convertToTimestamp(Object object) throws SQLException {
        if (object == null) {
            return null;
        }
        try {
            return new Timestamp(toMillis(
                    Converters.getConverter(object.getClass()).asTimestampWithTimezone(object).toEpochSecond()));
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    static Time convertToTime(Object object) throws SQLException {
        if (object == null) {
            return null;
        }
        try {
            return new Time(toMillis(
                    Converters.getConverter(object.getClass()).asTimestamp(object).toEpochSecond(ZoneOffset.UTC)));
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    static Date convertToDate(Object object)  throws SQLException  {
        if (object == null) {
            return null;
        }
        try {
            return new Date(toMillis(
                    Converters.getConverter(object.getClass()).asDate(object).atStartOfDay().toEpochSecond(ZoneOffset.UTC)));
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    private static long toMillis(long seconds) {
        return seconds * MILLIS_IN_SECONDS;
    }
}
