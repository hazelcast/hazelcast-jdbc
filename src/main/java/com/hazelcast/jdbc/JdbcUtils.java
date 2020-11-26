package com.hazelcast.jdbc;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

class JdbcUtils {

    static <T> T unwrap(Object target, Class<T> iface) throws SQLException {
        return iface.cast(target);
    }

    static boolean isWrapperFor(Object target, Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(target.getClass());
    }

    public static SQLFeatureNotSupportedException unsupported(String message) {
        return new SQLFeatureNotSupportedException(message);
    }
}
