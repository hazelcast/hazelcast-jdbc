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
