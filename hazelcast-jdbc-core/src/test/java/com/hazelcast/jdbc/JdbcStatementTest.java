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

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.QueryException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JdbcStatementTest {

    @Mock
    private HazelcastSqlClient client;
    @Mock
    private Connection connection;

    @Test
    void shouldThrowExceptionIfStatementIsClosed() throws SQLException {
        JdbcStatement statement = new JdbcStatement(client, connection);
        statement.close();

        assertThatThrownBy(statement::getResultSet)
                .isInstanceOf(SQLException.class)
                .hasMessage("Statement is closed");
    }

    @Test
    void shouldCloseResultSetOnUpdateQuery() throws SQLException {
        when(client.execute(any())).thenReturn(updateResult());
        JdbcStatement statement = new JdbcStatement(client, connection);
        boolean execute = statement.execute("UPDATE person SET name='JOHN' WHERE age=10");
        assertThat(execute).isFalse();
        assertThat(statement.getUpdateCount()).isEqualTo(3);
        assertThat(statement.getResultSet()).isNull();
    }

    @Test
    void shouldFailForQueryOnSqlUpdate() throws SQLException {
        when(client.execute(any())).thenThrow(new HazelcastSqlException(
                UuidUtil.newUnsecureUUID(), -1, "The statement doesn't produce rows", QueryException.error(""), null));
        JdbcStatement statement = new JdbcStatement(client, connection);

        assertThatThrownBy(() -> statement.executeQuery("UPDATE person SET name='JOHN' WHERE age=10"))
                .isInstanceOf(SQLException.class)
                .hasMessage("The statement doesn't produce rows");
    }

    @Test
    void shouldFailForUpdateOnSqlQuery() throws SQLException {
        when(client.execute(any())).thenThrow(new HazelcastSqlException(
                UuidUtil.newUnsecureUUID(), -1, "The statement doesn't produce update count", QueryException.error(""), null));
        JdbcStatement statement = new JdbcStatement(client, connection);

        assertThatThrownBy(() -> statement.executeUpdate("SELECT * FROM person"))
                .isInstanceOf(SQLException.class)
                .hasMessage("The statement doesn't produce update count");
    }

    @Test
    void shouldFailForExecuteOnPreparedStatement() throws SQLException {
        Statement statement = new JdbcPreparedStatement("SELECT * FROM person", client, connection);

        assertThatThrownBy(() -> statement.executeQuery("SELECT * FROM person"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Method not supported by PreparedStatement");

        assertThatThrownBy(() -> statement.executeUpdate("SELECT * FROM person"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Method not supported by PreparedStatement");

        assertThatThrownBy(() -> statement.execute("SELECT * FROM person"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Method not supported by PreparedStatement");

        verify(client, never()).execute(any());
    }

    @Test
    void shouldSetTimeoutAndBufferSize() throws SQLException {
        ArgumentCaptor<SqlStatement> statementArgumentCaptor = ArgumentCaptor.forClass(SqlStatement.class);
        when(client.execute(any())).thenReturn(queryResult());

        Statement statement = new JdbcStatement(client, connection);
        statement.setQueryTimeout(5);
        statement.setFetchSize(3);

        boolean execute = statement.execute("SELECT * FROM person");
        assertThat(execute).isTrue();

        verify(client).execute(statementArgumentCaptor.capture());
        SqlStatement executedStatement = statementArgumentCaptor.getValue();

        assertThat(executedStatement.getTimeoutMillis()).isEqualTo(5_000L);
        assertThat(executedStatement.getCursorBufferSize()).isEqualTo(3);
    }

    @Test
    void shouldOnlySupportValidFetchDirection() throws SQLException {
        Statement statement = new JdbcStatement(client, connection);

        statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        assertThat(statement.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);

        statement.setFetchDirection(ResultSet.FETCH_REVERSE);
        assertThat(statement.getFetchDirection()).isEqualTo(ResultSet.FETCH_REVERSE);

        statement.setFetchDirection(ResultSet.FETCH_UNKNOWN);
        assertThat(statement.getFetchDirection()).isEqualTo(ResultSet.FETCH_UNKNOWN);

        //noinspection MagicConstant
        assertThatThrownBy(() -> statement.setFetchDirection(3))
                .isInstanceOf(SQLException.class)
                .hasMessage("Invalid fetch direction value: 3");
    }

    @Test
    void shouldSupportRowPositionMethods() throws SQLException {
        when(client.execute(any())).thenReturn(
                queryResult(Arrays.asList(mock(SqlRow.class), mock(SqlRow.class), mock(SqlRow.class), mock(SqlRow.class))));
        Statement statement = new JdbcStatement(client, connection);
        statement.setMaxRows(2);
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person");

        assertThat(resultSet.getRow()).isEqualTo(0);
        assertThat(resultSet.isBeforeFirst()).isTrue();
        assertThat(resultSet.isFirst()).isFalse();
        assertThat(resultSet.isAfterLast()).isFalse();

        assertThat(resultSet.next()).isTrue();

        assertThat(resultSet.getRow()).isEqualTo(1);
        assertThat(resultSet.isBeforeFirst()).isFalse();
        assertThat(resultSet.isFirst()).isTrue();
        assertThat(resultSet.isAfterLast()).isFalse();

        assertThat(resultSet.next()).isTrue();

        assertThat(resultSet.getRow()).isEqualTo(2);
        assertThat(resultSet.isBeforeFirst()).isFalse();
        assertThat(resultSet.isFirst()).isFalse();
        assertThat(resultSet.isAfterLast()).isFalse();

        assertThat(resultSet.next()).isFalse();

        assertThat(resultSet.getRow()).isEqualTo(0);
        assertThat(resultSet.isBeforeFirst()).isFalse();
        assertThat(resultSet.isFirst()).isFalse();
        assertThat(resultSet.isAfterLast()).isTrue();
    }

    private SqlResult updateResult() {
        return new SqlResult() {
            @Override
            public SqlRowMetadata getRowMetadata() {
                return mock(SqlRowMetadata.class);
            }

            @Override
            public Iterator<SqlRow> iterator() {
                return Collections.emptyIterator();
            }

            @Override
            public long updateCount() {
                return 3;
            }

            @Override
            public void close() {
            }
        };
    }

    private SqlResult queryResult() {
        return queryResult(Collections.singletonList(mock(SqlRow.class)));
    }

    private SqlResult queryResult(List<SqlRow> rows) {
        return new SqlResult() {
            @Override
            public SqlRowMetadata getRowMetadata() {
                return mock(SqlRowMetadata.class);
            }

            @Override
            public Iterator<SqlRow> iterator() {
                return rows.iterator();
            }

            @Override
            public long updateCount() {
                return -1;
            }

            @Override
            public void close() {
            }
        };
    }
}
