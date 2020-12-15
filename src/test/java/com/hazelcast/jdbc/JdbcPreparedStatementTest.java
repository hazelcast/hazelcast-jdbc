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

    @Test
    void shouldFailOfNegativeParameterIndex() throws SQLException {
        JdbcPreparedStatement statement = new JdbcPreparedStatement("SELECT * FROM person WHERE name=?", client, connection);
        assertThatThrownBy(() -> statement.setString(-1, "John"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Parameter index should be greater than zero");
    }
}