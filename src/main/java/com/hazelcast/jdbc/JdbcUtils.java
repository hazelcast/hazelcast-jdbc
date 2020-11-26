package com.hazelcast.jdbc;

import java.sql.SQLException;

class JdbcUtils {

    static <T> T unwrap(Object target, Class<T> iface) throws SQLException {
        return iface.cast(target);
    }

    static boolean isWrapperFor(Object target, Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(target.getClass());
    }
}
