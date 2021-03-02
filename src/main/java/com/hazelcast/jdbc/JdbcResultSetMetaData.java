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

import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRowMetadata;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class JdbcResultSetMetaData implements ResultSetMetaData {

    private static final String NOT_APPLICABLE = "";

    private static final Map<SqlColumnType, Integer> SQL_TYPES_MAPPING = new HashMap<>();
    private static final Map<SqlColumnType, SqlTypeInfo> SQL_TYPES_INFO = new HashMap<>();

    static {
        initSqlTypesMapping();
        initSqlTypesInfo();
    }

    private final SqlRowMetadata sqlRowMetadata;

    JdbcResultSetMetaData(SqlRowMetadata sqlRowMetadata) {
        this.sqlRowMetadata = sqlRowMetadata;
    }

    private static void initSqlTypesMapping() {
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

    private static void initSqlTypesInfo() {
        SQL_TYPES_INFO.put(SqlColumnType.INTEGER, new SqlTypeInfo(Constants.INTEGER_DISPLAY_SIZE,
                Constants.INTEGER_DISPLAY_SIZE, Constants.ZERO));
        SQL_TYPES_INFO.put(SqlColumnType.BIGINT, new SqlTypeInfo(Constants.BIGINT_DISPLAY_SIZE,
                Constants.BIGINT_DISPLAY_SIZE, Constants.ZERO));
        SQL_TYPES_INFO.put(SqlColumnType.VARCHAR, new SqlTypeInfo(Constants.STRING_DISPLAY_SIZE,
                Constants.MAX_STRING_LENGTH, Constants.ZERO));
        SQL_TYPES_INFO.put(SqlColumnType.BOOLEAN, new SqlTypeInfo(Constants.BOOLEAN_DISPLAY_SIZE, Constants.BOOLEAN_PRECISION,
                Constants.ZERO));
        SQL_TYPES_INFO.put(SqlColumnType.TINYINT, new SqlTypeInfo(Constants.TINYINT_DISPLAY_SIZE, Constants.TINYINT_PRECISION,
                Constants.ZERO));
        SQL_TYPES_INFO.put(SqlColumnType.SMALLINT, new SqlTypeInfo(Constants.SMALLINT_DISPLAY_SIZE,
                Constants.SMALLINT_PRECISION, Constants.ZERO));
        SQL_TYPES_INFO.put(SqlColumnType.REAL, new SqlTypeInfo(Constants.REAL_DISPLAY_SIZE,
                Constants.REAL_PRECISION, Constants.REAL_PRECISION));
        SQL_TYPES_INFO.put(SqlColumnType.DOUBLE, new SqlTypeInfo(Constants.DOUBLE_DISPLAY_SIZE, Constants.DOUBLE_PRECISION,
                Constants.DOUBLE_PRECISION));
        SQL_TYPES_INFO.put(SqlColumnType.DECIMAL, new SqlTypeInfo(Constants.DECIMAL_DISPLAY_SIZE, Constants.DECIMAL_PRECISION,
                Constants.DECIMAL_PRECISION));
        SQL_TYPES_INFO.put(SqlColumnType.NULL, new SqlTypeInfo(Constants.NULL_DISPLAY_SIZE,
                Constants.BOOLEAN_PRECISION, Constants.ZERO));
        SQL_TYPES_INFO.put(SqlColumnType.OBJECT, new SqlTypeInfo(Constants.MAX_STRING_LENGTH,
                Constants.MAX_STRING_LENGTH, Constants.ZERO));
        SQL_TYPES_INFO.put(SqlColumnType.DATE, new SqlTypeInfo(Constants.DATE_DISPLAY_SIZE,
                Constants.DATE_DISPLAY_SIZE, Constants.ZERO));
        SQL_TYPES_INFO.put(SqlColumnType.TIME, new SqlTypeInfo(Constants.TIME_DISPLAY_SIZE,
                Constants.TIME_DISPLAY_SIZE, Constants.ZERO));
        SQL_TYPES_INFO.put(SqlColumnType.TIMESTAMP, new SqlTypeInfo(Constants.TIMESTAMP_DISPLAY_SIZE,
                Constants.TIMESTAMP_DISPLAY_SIZE, Constants.ZERO));
        SQL_TYPES_INFO.put(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, new SqlTypeInfo(Constants.TIMESTAMP_DISPLAY_SIZE,
                Constants.TIMESTAMP_DISPLAY_SIZE, Constants.ZERO));
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
        SqlColumnType type = getColumn(column).getType();
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case REAL:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return SQL_TYPES_INFO.get(getColumn(column).getType()).displaySize;
    }

    @Override
    public String getColumnLabel(int column) {
        return getColumn(column).getName();
    }

    @Override
    public String getColumnName(int column) {
        return getColumn(column).getName();
    }

    @Override
    public String getSchemaName(int column) {
        return NOT_APPLICABLE;
    }

    @Override
    public int getPrecision(int column) {
        return SQL_TYPES_INFO.get(getColumn(column).getType()).precision;
    }

    @Override
    public int getScale(int column) {
        return SQL_TYPES_INFO.get(getColumn(column).getType()).scale;
    }

    @Override
    public String getTableName(int column) {
        return NOT_APPLICABLE;
    }

    @Override
    public String getCatalogName(int column) {
        return NOT_APPLICABLE;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        SqlColumnType hzType = getColumn(column).getType();
        Integer jdbcType = SQL_TYPES_MAPPING.get(hzType);
        if (jdbcType == null) {
            throw new SQLException("Type mapping not found for type: " + hzType);
        }
        return jdbcType;
    }

    @Override
    public String getColumnTypeName(int column) {
        return getColumn(column).getType().name();
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
        return getColumn(column).getType().getValueClass().getName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return JdbcUtils.isWrapperFor(this, iface);
    }

    private SqlColumnMetadata getColumn(int column) {
        return sqlRowMetadata.getColumn(column - 1);
    }

    private static final class SqlTypeInfo {
        private final Integer displaySize;
        private final Integer precision;
        private final Integer scale;

        private SqlTypeInfo(Integer displaySize, Integer precision, Integer scale) {
            this.displaySize = displaySize;
            this.precision = precision;
            this.scale = scale;
        }
    }
}
