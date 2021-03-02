package com.hazelcast.jdbc;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DriverTest {

    private static final Condition<Throwable> THROWN_IN_DRIVER_CONNECT = new Condition<Throwable>(t -> {
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
