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

import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JdbcResultSetTest {

    @Mock
    private SqlResult sqlResult;
    @Mock
    private SqlRow sqlRow;
    @Mock
    private JdbcStatement statement;

    private JdbcResultSet resultSet;

    @BeforeEach
    public void setUp() throws SQLException {
        when(sqlResult.iterator()).thenReturn(Collections.singletonList(sqlRow).iterator());
        resultSet = new JdbcResultSet(sqlResult, statement);
    }

    @Test
    public void shouldReturnTrueWhenFieldWasNull() throws SQLException {
        when(sqlResult.getRowMetadata()).thenReturn(new SqlRowMetadata(Collections.singletonList(
                new SqlColumnMetadata("name", SqlColumnType.VARCHAR, false))));

        when(sqlRow.getObject(anyInt())).thenReturn(null);
        resultSet.next();
        String name = resultSet.getString("name");
        assertThat(name).isNull();
        assertThat(resultSet.wasNull()).isTrue();
    }

    @Test
    void shouldThrowExceptionIfColumnNotFound() {
        when(sqlResult.getRowMetadata()).thenReturn(new SqlRowMetadata(Collections.singletonList(
                new SqlColumnMetadata("name", SqlColumnType.VARCHAR, false))));

        assertThatThrownBy(() -> resultSet.findColumn("surname"))
                .isInstanceOf(SQLException.class)
                .hasMessage("ResultSet does not contain column \"surname\"");
    }

    @Test
    void shouldThrowExceptionOnGetByColumnLabelIfColumnNotFound() {
        when(sqlResult.getRowMetadata()).thenReturn(new SqlRowMetadata(Collections.singletonList(
                new SqlColumnMetadata("name", SqlColumnType.VARCHAR, false))));

        assertThatThrownBy(() -> resultSet.getString("address"))
                .isInstanceOf(SQLException.class)
                .hasMessage("ResultSet does not contain column \"address\"");
    }

    @Test
    void shouldThrowExceptionOnGetByColumnIndexIfColumnNotFound() {
        when(sqlResult.getRowMetadata()).thenReturn(new SqlRowMetadata(Collections.singletonList(
                new SqlColumnMetadata("name", SqlColumnType.VARCHAR, false))));

        assertThatThrownBy(() -> resultSet.getString(2))
                .isInstanceOf(SQLException.class)
                .hasMessage("ResultSet does not contain column with index 2");
    }

    @Test
    void shouldOnlySupportValidFetchDirection() throws SQLException {
        resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
        assertThat(resultSet.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);

        resultSet.setFetchDirection(ResultSet.FETCH_REVERSE);
        assertThat(resultSet.getFetchDirection()).isEqualTo(ResultSet.FETCH_REVERSE);

        resultSet.setFetchDirection(ResultSet.FETCH_UNKNOWN);
        assertThat(resultSet.getFetchDirection()).isEqualTo(ResultSet.FETCH_UNKNOWN);

        //noinspection MagicConstant
        assertThatThrownBy(() -> resultSet.setFetchDirection(3))
                .isInstanceOf(SQLException.class)
                .hasMessage("Invalid fetch direction value: 3");
    }

    @Test
    void shouldCloseResultSet() throws SQLException {
        resultSet.close();
        assertThat(resultSet.isClosed()).isTrue();

        assertThatThrownBy(() -> resultSet.getLong(1))
                .isInstanceOf(SQLException.class)
                .hasMessage("Result set is closed");
    }

    @Test
    void shouldSetDefaultFetchSizeFromStatement() throws SQLException {
        when(statement.getFetchSize()).thenReturn(5);

        assertThat(resultSet.getFetchSize()).isEqualTo(5);
    }
}