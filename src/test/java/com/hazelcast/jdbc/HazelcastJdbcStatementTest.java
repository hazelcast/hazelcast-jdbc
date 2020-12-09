package com.hazelcast.jdbc;

import com.hazelcast.core.HazelcastInstance;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
}