package com.hazelcast.jdbc;

import java.sql.SQLFeatureNotSupportedException;

class JdbcUtils {

    static <T> T unwrap(Object target, Class<T> iface) {
        return iface.cast(target);
    }

    static boolean isWrapperFor(Object target, Class<?> iface) {
        return iface.isAssignableFrom(target.getClass());
    }

    public static SQLFeatureNotSupportedException unsupported(String message) {
        return new SQLFeatureNotSupportedException(message);
    }
}
