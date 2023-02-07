/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

public class JdbcResultSetMetaData implements ResultSetMetaData {

    private static final String NOT_APPLICABLE = "";

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
        SqlColumnType type = getColumn(column).getType();
        return TypeUtil.getTypeInfo(type).isSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return TypeUtil.getTypeInfo(getColumn(column).getType()).getDisplaySize();
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
        return TypeUtil.getTypeInfo(getColumn(column).getType()).getPrecision();
    }

    @Override
    public int getScale(int column) {
        return TypeUtil.getTypeInfo(getColumn(column).getType()).getScale();
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
        Integer jdbcType = TypeUtil.getJdbcType(hzType);
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
        // We have to check the range here, even though sqlRowMetadata.getColumn() checks it too, to
        // throw an exception where 1 is not subtracted from the column index
        if (column <= 0 || column > getColumnCount()) {
            throw new IndexOutOfBoundsException("Column index is out of bounds: " + column);
        }
        return sqlRowMetadata.getColumn(column - 1);
    }
}
