package com.hazelcast.jdbc;

import com.hazelcast.core.HazelcastInstance;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLFeatureNotSupportedException;

@ExtendWith(MockitoExtension.class)
public class HazelcastJdbcPreparedStatementTest {

    @Mock
    private HazelcastJdbcClient client;
    @Mock
    private Connection connection;

    @Test
    void shouldThrowUnsupportedForUpdateQuery() {
        PreparedStatement statement = new HazelcastJdbcPreparedStatement("INSERT INTO person VALUES (?, ?, ?)", client, connection);
        Assertions.assertThatThrownBy(statement::executeUpdate)
                .isInstanceOf(SQLFeatureNotSupportedException.class)
                .hasMessage("Updates not supported");
    }
}