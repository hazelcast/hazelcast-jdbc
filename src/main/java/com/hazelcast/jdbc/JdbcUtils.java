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

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

final class JdbcUtils {

    private JdbcUtils() {
        throw new UnsupportedOperationException();
    }

    static <T> T unwrap(Object target, Class<T> iface) {
        return iface.cast(target);
    }

    static boolean isWrapperFor(Object target, Class<?> iface) {
        return iface.isAssignableFrom(target.getClass());
    }

    public static SQLFeatureNotSupportedException unsupported(String message) {
        return new SQLFeatureNotSupportedException(message);
    }

    public static SQLException sqlException(String message) {
        return new SQLException(message);
    }
}
