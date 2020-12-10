package com.hazelcast.jdbc;

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

class JdbcConnection implements Connection {

    private final HazelcastJdbcClient client;

    /** Is connection closed. */
    private boolean closed;

    /** DB Schema */
    private String schema;

    /** Read-only flag. JDBC driver doesn't use it except for the getter/setter. */
    private boolean readOnly = true;

    /** Auto-commit flag */
    private boolean autoCommit;

    JdbcConnection(HazelcastJdbcClient client) {
        this.client = client;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new HazelcastJdbcStatement(client, this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new HazelcastJdbcPreparedStatement(sql, client, this);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        checkClosed();
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
    public void close() throws SQLException {
        if (!isClosed()) {
            client.shutdown();
            closed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        throw JdbcUtils.unsupported("DatabaseMetaData not supported");
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
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        throw JdbcUtils.unsupported("CallableStatement not supported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkClosed();
        throw JdbcUtils.unsupported("Type Map not supported");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        throw JdbcUtils.unsupported("Type Map not supported");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw JdbcUtils.unsupported("Value for holdability not supported" + holdability);
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        checkClosed();
        throw JdbcUtils.unsupported("Savepoint is not supported.");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkClosed();
        throw JdbcUtils.unsupported("Savepoint is not supported.");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkClosed();
        throw JdbcUtils.unsupported("Rollback is not supported.");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkClosed();
        throw JdbcUtils.unsupported("Savepoint is not supported.");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkClosed();
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkClosed();
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkClosed();
        throw JdbcUtils.unsupported("CallableStatement not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        throw unsupportedAutoGeneratedKeys();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        throw unsupportedAutoGeneratedKeys();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        throw unsupportedAutoGeneratedKeys();
    }

    @Override
    public Clob createClob() throws SQLException {
        checkClosed();
        throw JdbcUtils.unsupported("Clob is not supported.");
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkClosed();
        throw JdbcUtils.unsupported("NClob is not supported.");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        checkClosed();
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
        checkClosed();
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkClosed();
        throw JdbcUtils.unsupported("Array is not supported.");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkClosed();
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
        checkClosed();
        throw JdbcUtils.unsupported("Network timeout is not supported");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkClosed();
        throw JdbcUtils.unsupported("Network timeout is not supported");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return JdbcUtils.isWrapperFor(this, iface);
    }

    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is closed", "STATE", -1);
        }
    }

    private SQLFeatureNotSupportedException unsupportedAutoGeneratedKeys() {
        return JdbcUtils.unsupported("Auto-generated keys are not supported.");
    }
}
