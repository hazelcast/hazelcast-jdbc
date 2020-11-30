package com.hazelcast.jdbc;

import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HazelcastJdbcResultSetTest {

    @Mock
    private SqlResult sqlResult;
    @Mock
    private SqlRow sqlRow;

    private HazelcastJdbcResultSet resultSet;

    @BeforeEach
    public void setUp() throws Exception {
        when(sqlResult.iterator()).thenReturn(Collections.singletonList(sqlRow).iterator());
        resultSet = new HazelcastJdbcResultSet(sqlResult);
    }

    @Test
    public void shouldReturnTrueWhenFieldWasNull() throws SQLException {
        resultSet.next();
        String name = resultSet.getString("name");
        assertThat(name).isNull();
        assertThat(resultSet.wasNull()).isTrue();
    }
}