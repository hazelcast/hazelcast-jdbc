package com.hazelcast.jdbc;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
public class JdbcPreparedStatementTest {

    @Mock
    private HazelcastSqlClient client;
    @Mock
    private Connection connection;

    @Test
    void shouldFailOnAddingMoreParametersThanAllowed() {
        JdbcPreparedStatement statement = new JdbcPreparedStatement("SELECT * FROM person WHERE name=?", client, connection);
        assertThatThrownBy(() -> statement.setString(2, "John"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Invalid parameter index value: 2");
    }

    @Test
    void shouldFailIfNotAllParametersWhereSet() throws SQLException {
        JdbcPreparedStatement statement = new JdbcPreparedStatement("SELECT * FROM person WHERE name=? AND age=?", client, connection);
        statement.setInt(2, 27);
        assertThatThrownBy(statement::execute)
                .isInstanceOf(SQLException.class)
                .hasMessage("Parameter #1 is not set");
    }
}