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

import com.hazelcast.sql.SqlExpectedResultType;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {

    private ParameterList parameters;
    private final String sql;

    JdbcPreparedStatement(String sql, HazelcastSqlClient client, Connection connection) throws SQLException {
        super(client, connection);
        this.sql = sql;
        parameters = new ParameterList();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        doExecute(sql, parameters.asParameters(), SqlExpectedResultType.ROWS);
        return resultSet;
    }

    @Override
    public int executeUpdate() throws SQLException {
        executeLargeUpdate();
        return getUpdateCount();
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        checkClosed();
        doExecute(sql, parameters.asParameters(), SqlExpectedResultType.UPDATE_COUNT);
        return updateCount;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException("Method not supported by PreparedStatement");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("Method not supported by PreparedStatement");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new SQLException("Method not supported by PreparedStatement");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkClosed();
        parameters.setNullValue(parameterIndex);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        throw JdbcUtils.unsupported("Date is not supported");
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw JdbcUtils.unsupported("Time is not supported");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        throw JdbcUtils.unsupported("Timestamp is not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw JdbcUtils.unsupported("Ascii stream not supported");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw JdbcUtils.unsupported("Unicode stream not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw JdbcUtils.unsupported("Binary stream not supported");
    }

    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        parameters = new ParameterList();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw JdbcUtils.unsupported("setObject with scaleOrLength not implemented");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        checkClosed();
        setObject(parameterIndex, TypeConverter.convertTo(x, targetSqlType));
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public boolean execute() throws SQLException {
        checkClosed();
        doExecute(sql, parameters.asParameters(), SqlExpectedResultType.ANY);
        return resultSet != null;
    }

    @Override
    public void addBatch() throws SQLException {
        checkClosed();
        throw unsupportedBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw JdbcUtils.unsupported("Character stream is not supported");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw JdbcUtils.unsupported("Ref is not supported");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw JdbcUtils.unsupported("Blob is not supported");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw JdbcUtils.unsupported("Clob is not supported");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw JdbcUtils.unsupported("Array is not supported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw JdbcUtils.unsupported("ResultSetMetaData not supported");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw JdbcUtils.unsupported("Date not supported");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw JdbcUtils.unsupported("Time not supported");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw JdbcUtils.unsupported("Timestamp not supported");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw JdbcUtils.unsupported("URL is not supported");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw JdbcUtils.unsupported("ParameterMetaData is not supported");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw JdbcUtils.unsupported("RowId is not supported");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw JdbcUtils.unsupported("NCharacter stream is not supported");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw JdbcUtils.unsupported("NClob stream is not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw JdbcUtils.unsupported("Clob stream is not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw JdbcUtils.unsupported("Blob stream is not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw JdbcUtils.unsupported("NClob stream is not supported");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw JdbcUtils.unsupported("SQLXML stream is not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw JdbcUtils.unsupported("Ascii stream is not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw JdbcUtils.unsupported("Binary stream is not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw JdbcUtils.unsupported("Character stream is not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw JdbcUtils.unsupported("Ascii stream is not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw JdbcUtils.unsupported("Binary stream is not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw JdbcUtils.unsupported("Character stream is not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw JdbcUtils.unsupported("NCharacter stream is not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw JdbcUtils.unsupported("Clob is not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw JdbcUtils.unsupported("Blob is not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw JdbcUtils.unsupported("NClob is not supported");
    }

    private void setParameter(int parameterIndex, Object parameter) throws SQLException {
        if (parameterIndex <= 0) {
            throw new SQLException("Parameter index should be greater than zero");
        }
        parameters.setParameter(parameterIndex, parameter);
    }
}
