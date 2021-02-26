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

import com.hazelcast.core.HazelcastInstance;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class JdbcConnection implements Connection {

    private final HazelcastSqlClient client;

    /**
     * Is connection closed.
     */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * DB Schema
     */
    private String schema;

    /**
     * Read-only flag. JDBC driver doesn't use it except for the getter/setter.
     */
    private boolean readOnly = true;

    /**
     * Auto-commit flag
     */
    private boolean autoCommit;

    JdbcConnection(HazelcastSqlClient client) {
        this.client = client;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new JdbcStatement(client, this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new JdbcPreparedStatement(sql, client, this);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw JdbcUtils.unsupported("CallableStatement not supported");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Auto-commit is set to true");
        }
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Auto-commit is set to true");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            client.shutdown();
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public DatabaseMetaData getMetaData() {
        return new JdbcDataBaseMetadata(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        switch (level) {
            case Connection.TRANSACTION_NONE:
            case Connection.TRANSACTION_READ_COMMITTED:
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
                return;
            default:
                throw new SQLException("Invalid value for transaction isolation: " + level);
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        checkStatementParameters(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        checkStatementParameters(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw JdbcUtils.unsupported("CallableStatement not supported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw JdbcUtils.unsupported("Type Map not supported");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw JdbcUtils.unsupported("Type Map not supported");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        if (!supportsHoldability(holdability)) {
            throw JdbcUtils.unsupported("Value for holdability not supported " + holdability);
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw JdbcUtils.unsupported("Savepoint is not supported.");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw JdbcUtils.unsupported("Savepoint is not supported.");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw JdbcUtils.unsupported("Rollback is not supported.");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw JdbcUtils.unsupported("Savepoint is not supported.");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        checkClosed();
        checkStatementParameters(resultSetType, resultSetConcurrency, resultSetHoldability);
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        checkClosed();
        checkStatementParameters(resultSetType, resultSetConcurrency, resultSetHoldability);
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw JdbcUtils.unsupported("CallableStatement not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw unsupportedAutoGeneratedKeys();
        }
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        if (columnIndexes.length != 0) {
            throw unsupportedAutoGeneratedKeys();
        }
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        if (columnNames.length != 0) {
            throw unsupportedAutoGeneratedKeys();
        }
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw JdbcUtils.unsupported("Clob is not supported.");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw JdbcUtils.unsupported("Blob is not supported");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw JdbcUtils.unsupported("NClob is not supported.");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw JdbcUtils.unsupported("SQLXML is not supported");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException("Timeout cannot be less than 0");
        }
        return !isClosed();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        if (isClosed()) {
            throw new SQLClientInfoException("Connection is closed", Collections.emptyMap());
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        if (isClosed()) {
            throw new SQLClientInfoException("Connection is closed", Collections.emptyMap());
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw JdbcUtils.unsupported("Client Info is not supported");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw JdbcUtils.unsupported("Client Info is not supported");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw JdbcUtils.unsupported("Array is not supported.");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw JdbcUtils.unsupported("Struct is not supported.");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return schema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (executor == null) {
            throw new SQLException("Executor cannot be null");
        }
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw JdbcUtils.unsupported("Network timeout is not supported");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw JdbcUtils.unsupported("Network timeout is not supported");
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return JdbcUtils.isWrapperFor(this, iface);
    }

    public boolean supportsResultSetType(int resultSetType) {
        return resultSetType == ResultSet.TYPE_FORWARD_ONLY;
    }

    public boolean supportsResultSetConcurrency(int resultSetConcurrency) {
        return resultSetConcurrency == ResultSet.CONCUR_READ_ONLY;
    }

    public boolean supportsHoldability(int holdability) {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    JdbcUrl getJdbcUrl() {
        return client.getJdbcUrl();
    }

    HazelcastInstance getClientInstance() {
        return client.getClient();
    }

    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is closed");
        }
    }

    private void checkStatementParameters(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        if (!supportsResultSetType(resultSetType)) {
            throw JdbcUtils.unsupported("Unsupported ResultSet type: " + resultSetType);
        }
        if (!supportsResultSetConcurrency(resultSetConcurrency)) {
            throw JdbcUtils.unsupported("Unsupported ResultSet concurrency: " + resultSetConcurrency);
        }
        if (!supportsHoldability(resultSetHoldability)) {
            throw JdbcUtils.unsupported("Unsupported ResultSet holdability: " + resultSetHoldability);
        }
    }

    private SQLFeatureNotSupportedException unsupportedAutoGeneratedKeys() {
        return JdbcUtils.unsupported("Auto-generated keys are not supported.");
    }
}
