package com.hazelcast.jdbc;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HazelcastJdbcStatementTest {

    @Mock
    private HazelcastJdbcClient client;
    @Mock
    private Connection connection;

    @Test
    void shouldThrowExceptionIfStatementIsClosed() throws SQLException {
        HazelcastJdbcStatement statement = new HazelcastJdbcStatement(client, connection);
        statement.close();

        assertThatThrownBy(statement::getResultSet)
                .isInstanceOf(SQLException.class)
                .hasMessage("Statement is closed");
    }

    @Test
    void shouldCloseResultSetOnUpdateQuery() throws SQLException {
        when(client.execute(any())).thenReturn(new SqlResult() {
            @SuppressWarnings("ConstantConditions")
            @Override
            public SqlRowMetadata getRowMetadata() {
                return null;
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
        });
        HazelcastJdbcStatement statement = new HazelcastJdbcStatement(client, connection);
        boolean execute = statement.execute("UPDATE person SET name='JOHN' WHERE age=10");
        assertThat(execute).isFalse();
        assertThat(statement.getUpdateCount()).isEqualTo(3);
        assertThat(statement.getResultSet()).isNull();
    }
}