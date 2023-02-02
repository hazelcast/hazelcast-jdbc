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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlExpectedResultType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlStatement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;

public class JdbcStatement implements Statement {

    /**
     * Current result as an update count.
     * Value -1 means that the result is not an update count but a result set.
     */
    long updateCount = -1;

    /** Current result as a result set */
    ResultSet resultSet;

    /** Query timeout in seconds. */
    private int queryTimeout;

    /** Fetch size. */
    private int fetchSize;

    /** Whether the statement is closed. */
    private boolean closed;

    /** Poolable flag. */
    private boolean poolable;

    /** Fetch direction hint. */
    private int fetchDirection = ResultSet.FETCH_FORWARD;

    /** Whether to close the statement when the result set is closed. */
    private boolean closeOnCompletion;

    /** Result set max rows */
    private int maxRows;

    private final HazelcastSqlClient client;
    private final Connection connection;
    private final String schema;

    JdbcStatement(HazelcastSqlClient client, Connection connection) throws SQLException {
        this.client = client;
        this.connection = connection;
        schema = connection.getSchema();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        doExecute(sql, Collections.emptyList(), SqlExpectedResultType.ROWS);
        return resultSet;
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        doExecute(sql, Collections.emptyList(), SqlExpectedResultType.UPDATE_COUNT);
        return getLargeUpdateCount();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        executeLargeUpdate(sql);
        return getUpdateCount();
    }

    @Override
    public void close() throws SQLException {
        if (!isClosed()) {
            closeResultSet();
            closed = true;
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkClosed();
        if (max < 0) {
            throw new SQLException("Invalid value for max field size: " + max);
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkClosed();
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        checkClosed();
        if (max < 0) {
            throw new SQLException("Invalid value for max rows: " + max);
        }
        maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkClosed();
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkClosed();
        if (seconds < 0) {
            throw new SQLException("Invalid value for query timeout seconds: " + seconds);
        }
        this.queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        throw JdbcUtils.unsupported("Cancellation is not supported");
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
    public void setCursorName(String name) throws SQLException {
        throw JdbcUtils.unsupported("Cursor Name is not supported");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        doExecute(sql, Collections.emptyList(), SqlExpectedResultType.ANY);
        return resultSet != null;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();

        if (updateCount != -1) {
            return null;
        }
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return Math.toIntExact(getLargeUpdateCount());
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        checkClosed();
        return updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        switch (direction) {
            case ResultSet.FETCH_FORWARD:
            case ResultSet.FETCH_REVERSE:
            case ResultSet.FETCH_UNKNOWN:
                this.fetchDirection = direction;
                break;
            default:
                throw new SQLException("Invalid fetch direction value: " + direction);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        if (rows < 0) {
            throw new SQLException("Invalid value for fetch size: " + rows);
        }
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw unsupportedBatch();
    }

    @Override
    public void clearBatch() throws SQLException {
        throw unsupportedBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw unsupportedBatch();
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        throw unsupportedBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkClosed();
        if (current == Statement.CLOSE_CURRENT_RESULT || current == Statement.CLOSE_ALL_RESULTS) {
            closeResultSet();
            updateCount = -1;
        }
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkClosed();
        return JdbcResultSet.EMPTY;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw unsupportedAutoGeneratedKeys();
        }
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        if (columnIndexes.length != 0) {
            throw unsupportedAutoGeneratedKeys();
        }
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        if (columnNames.length != 0) {
            throw unsupportedAutoGeneratedKeys();
        }
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw unsupportedAutoGeneratedKeys();
        }
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        if (columnIndexes.length != 0) {
            throw unsupportedAutoGeneratedKeys();
        }
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        if (columnNames.length != 0) {
            throw unsupportedAutoGeneratedKeys();
        }
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkClosed();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
        this.poolable = poolable;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkClosed();
        return poolable;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkClosed();
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkClosed();
        return closeOnCompletion;
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return JdbcUtils.isWrapperFor(this, iface);
    }

    void tryCloseOnCompletion() throws SQLException {
        if (closeOnCompletion) {
            close();
        }
    }

    void doExecute(String sql, List<Object> parameters, SqlExpectedResultType expectedResult) throws SQLException {
        checkClosed();

        SqlStatement query = new SqlStatement(sql)
                .setParameters(parameters)
                .setExpectedResultType(expectedResult)
                .setSchema(schema);
        if (queryTimeout != 0) {
            query.setTimeoutMillis(SECONDS.toMillis(queryTimeout));
        }
        if (fetchSize != 0) {
            query.setCursorBufferSize(fetchSize);
        }
        try {
            SqlResult sqlResult = client.execute(query);
            if (sqlResult.isRowSet()) {
                resultSet = new JdbcResultSet(sqlResult, this);
                updateCount = -1;
            } else {
                updateCount = sqlResult.updateCount();
                closeResultSet();
            }
        } catch (HazelcastSqlException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement is closed");
        }
    }

    SQLException unsupportedBatch() {
        return JdbcUtils.unsupported("Batch updates is not supported");
    }

    private SQLFeatureNotSupportedException unsupportedAutoGeneratedKeys() {
        return JdbcUtils.unsupported("Auto-generated keys are not supported.");
    }

    private void closeResultSet() throws SQLException {
        if (resultSet != null) {
            resultSet.close();
            resultSet = null;
        }
    }
}
