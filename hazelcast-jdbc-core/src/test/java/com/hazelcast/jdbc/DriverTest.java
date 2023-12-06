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

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DriverTest {

    private static final Condition<Throwable> THROWN_IN_DRIVER_CONNECT = new Condition<>(t -> {
        StackTraceElement[] stackTrace = t.getStackTrace();
        return stackTrace[0].getClassName().equals(Driver.class.getName())
               && stackTrace[0].getMethodName().equals("connect");
    }, "thrown at Driver.connect()");

    @Test
    void when_badUrl_then_ourError() {
        assertThatThrownBy(() -> DriverManager.getConnection("jdbc:hazelcast:foo"))
                .isInstanceOf(SQLException.class)
                .is(THROWN_IN_DRIVER_CONNECT);
    }

    @Test
    void when_nonHzUrl_then_notHandledByOurDriver() {
        assertThatThrownBy(() -> DriverManager.getConnection("jdbc:fooDatabase:bar"))
                .isInstanceOf(SQLException.class)
                .isNot(THROWN_IN_DRIVER_CONNECT);
    }
}
