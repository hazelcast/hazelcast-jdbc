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
import com.hazelcast.sql.SqlRowMetadata;

import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class JdbcResultSetMetaData implements ResultSetMetaData {

    private static final String NOT_APPLICABLE_STRING = "";
    private static final int NOT_APPLICABLE_NUMBER = 0;
    private static final Map<SqlColumnType, Integer> SQL_TYPES_MAPPING = new HashMap<>();
    private static final Map<SqlColumnType, Integer> SQL_TYPES_MAX_DISPLAY_WIDTH = new HashMap<>();

    static {
        setSqlTypesMapping();
        setSqlTypesMaxDisplayWidth();
    }

    private static void setSqlTypesMapping() {
        SQL_TYPES_MAPPING.put(SqlColumnType.VARCHAR, Types.VARCHAR);
        SQL_TYPES_MAPPING.put(SqlColumnType.BOOLEAN, Types.BOOLEAN);
        SQL_TYPES_MAPPING.put(SqlColumnType.TINYINT, Types.TINYINT);
        SQL_TYPES_MAPPING.put(SqlColumnType.SMALLINT, Types.SMALLINT);
        SQL_TYPES_MAPPING.put(SqlColumnType.INTEGER, Types.INTEGER);
        SQL_TYPES_MAPPING.put(SqlColumnType.BIGINT, Types.BIGINT);
        SQL_TYPES_MAPPING.put(SqlColumnType.DECIMAL, Types.DECIMAL);
        SQL_TYPES_MAPPING.put(SqlColumnType.REAL, Types.REAL);
        SQL_TYPES_MAPPING.put(SqlColumnType.DOUBLE, Types.DOUBLE);
        SQL_TYPES_MAPPING.put(SqlColumnType.DATE, Types.DATE);
        SQL_TYPES_MAPPING.put(SqlColumnType.TIME, Types.TIME);
        SQL_TYPES_MAPPING.put(SqlColumnType.TIMESTAMP, Types.TIMESTAMP);
        SQL_TYPES_MAPPING.put(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, Types.TIMESTAMP_WITH_TIMEZONE);
        SQL_TYPES_MAPPING.put(SqlColumnType.OBJECT, Types.JAVA_OBJECT);
        SQL_TYPES_MAPPING.put(SqlColumnType.NULL, Types.NULL);
    }

    private static void setSqlTypesMaxDisplayWidth() {
        SQL_TYPES_MAX_DISPLAY_WIDTH.put(SqlColumnType.INTEGER, 11); //Integer.MIN_VALUE character count
        SQL_TYPES_MAX_DISPLAY_WIDTH.put(SqlColumnType.BIGINT, 20); //Long.MIN_VALUE character count
        SQL_TYPES_MAX_DISPLAY_WIDTH.put(SqlColumnType.VARCHAR, Integer.MAX_VALUE);
        SQL_TYPES_MAX_DISPLAY_WIDTH.put(SqlColumnType.BOOLEAN, 5);
        SQL_TYPES_MAX_DISPLAY_WIDTH.put(SqlColumnType.TINYINT, 4); //Byte.MIN_VALUE character count
        SQL_TYPES_MAX_DISPLAY_WIDTH.put(SqlColumnType.SMALLINT, 6); //Short.MIN_VALUE character count
        SQL_TYPES_MAX_DISPLAY_WIDTH.put(SqlColumnType.REAL, 15);
        SQL_TYPES_MAX_DISPLAY_WIDTH.put(SqlColumnType.DOUBLE, 24);
        SQL_TYPES_MAX_DISPLAY_WIDTH.put(SqlColumnType.NULL, 4);
        SQL_TYPES_MAX_DISPLAY_WIDTH.put(SqlColumnType.OBJECT, Integer.MAX_VALUE);
        SQL_TYPES_MAX_DISPLAY_WIDTH.put(SqlColumnType.DATE, 10);
        SQL_TYPES_MAX_DISPLAY_WIDTH.put(SqlColumnType.TIME, 8);
        SQL_TYPES_MAX_DISPLAY_WIDTH.put(SqlColumnType.TIMESTAMP, 19);
        SQL_TYPES_MAX_DISPLAY_WIDTH.put(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, 14);
        SQL_TYPES_MAX_DISPLAY_WIDTH.put(SqlColumnType.DECIMAL, 100_000);
    }

    private final SqlRowMetadata sqlRowMetadata;

    JdbcResultSetMetaData(SqlRowMetadata sqlRowMetadata) {
        this.sqlRowMetadata = sqlRowMetadata;
    }

    @Override
    public int getColumnCount() {
        return sqlRowMetadata.getColumnCount();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return true;
    }

    @Override
    public boolean isSearchable(int column) {
        return true;
    }

    @Override
    public boolean isCurrency(int column) {
        return false;
    }

    @Override
    public int isNullable(int column) {
        return ResultSetMetaData.columnNullable;
    }

    @Override
    public boolean isSigned(int column) {
        SqlColumnType type = sqlRowMetadata.getColumn(column).getType();
        switch (type) {
            case REAL:
            case BIGINT:
            case DOUBLE:
            case INTEGER:
            case TINYINT:
            case DECIMAL:
            case SMALLINT:
                return true;
            default:
                return false;
        }
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return SQL_TYPES_MAX_DISPLAY_WIDTH.get(sqlRowMetadata.getColumn(column).getType());
    }

    @Override
    public String getColumnLabel(int column) {
        return sqlRowMetadata.getColumn(column).getName();
    }

    @Override
    public String getColumnName(int column) {
        return sqlRowMetadata.getColumn(column).getName();
    }

    @Override
    public String getSchemaName(int column) {
        return NOT_APPLICABLE_STRING;
    }

    @Override
    public int getPrecision(int column) {
        return NOT_APPLICABLE_NUMBER;
    }

    @Override
    public int getScale(int column) {
        return NOT_APPLICABLE_NUMBER;
    }

    @Override
    public String getTableName(int column) {
        return NOT_APPLICABLE_STRING;
    }

    @Override
    public String getCatalogName(int column) {
        return NOT_APPLICABLE_STRING;
    }

    @Override
    public int getColumnType(int column) {
        return SQL_TYPES_MAPPING.getOrDefault(sqlRowMetadata.getColumn(column).getType(), Types.OTHER);
    }

    @Override
    public String getColumnTypeName(int column) {
        return sqlRowMetadata.getColumn(column).getType().name();
    }

    @Override
    public boolean isReadOnly(int column) {
        return true;
    }

    @Override
    public boolean isWritable(int column) {
        return !isReadOnly(column);
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    @Override
    public String getColumnClassName(int column) {
        return sqlRowMetadata.getColumn(column).getType().getValueClass().getName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return JdbcUtils.isWrapperFor(this, iface);
    }
}
