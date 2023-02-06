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

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ParameterListTest {

    @Test
    void testAddingParams() throws SQLException {
        ParameterList pl = new ParameterList();
        pl.setParameter(2, 12);
        assertThatThrownBy(pl::asParameters)
                .isInstanceOf(SQLException.class)
                .hasMessage("Parameter #1 is not set");
        pl.setParameter(1, 11);
        assertEquals(asList(11, 12), pl.asParameters());
    }
}
