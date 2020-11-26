package com.hazelcast.jdbc;

import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HazelcastJdbcResultSetTest {

    @Mock
    private SqlResult sqlResult;
    @Mock
    private SqlRow sqlRow;

    private HazelcastJdbcResultSet resultSet;

    @Before
    public void setUp() throws Exception {
        when(sqlRow.getObject(any()))
                .thenReturn(null);
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

    @Test
    public void shouldFallByTimeoutIfQueryExecutesLonger() {
    }
}