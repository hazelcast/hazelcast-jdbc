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

import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Iterator;

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
    void shouldFailForQueryOnSqlUpdate() {
        when(client.execute(any())).thenReturn(updateResult());
        JdbcStatement statement = new JdbcStatement(client, connection);

        assertThatThrownBy(() -> statement.executeQuery("UPDATE person SET name='JOHN' WHERE age=10"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Invalid SQL statement");
    }

    @Test
    void shouldFailForUpdateOnSqlQuery() {
        when(client.execute(any())).thenReturn(queryResult());
        JdbcStatement statement = new JdbcStatement(client, connection);

        assertThatThrownBy(() -> statement.executeUpdate("SELECT * FROM person"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Invalid SQL statement");
    }

    @Test
    void shouldFailForExecuteOnPreparedStatement() {
        Statement statement = new JdbcPreparedStatement("SELECT * FROM person", client, connection);

        assertThatThrownBy(() -> statement.executeQuery("SELECT * FROM person"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Method not supported by PreparedStatement");
        verify(client, never()).execute(any());
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
        return new SqlResult() {
            @Override
            public SqlRowMetadata getRowMetadata() {
                return mock(SqlRowMetadata.class);
            }

            @Override
            public Iterator<SqlRow> iterator() {
                return Collections.singletonList(mock(SqlRow.class)).iterator();
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