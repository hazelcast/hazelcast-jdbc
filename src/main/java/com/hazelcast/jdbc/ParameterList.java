package com.hazelcast.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Wraps the list of parameters used for {@link java.sql.PreparedStatement}.
 * Validates all the parameters are set and to the correct {@literal parameterIndex}.
 */
class ParameterList {

    private static final Parameter NULL_VALUE = new Parameter(null);

    private final List<Parameter> parameters;

    ParameterList(String sql) {
        int parametersNumber = Math.toIntExact(sql.chars().filter(c -> c == '?').count());
        parameters = new ArrayList<>(Collections.nCopies(parametersNumber, null));
    }

    List<Object> asParameters() throws SQLException {
        if (parameters.isEmpty()) {
            return Collections.emptyList();
        }
        List<Object> params = new ArrayList<>(parameters.size());
        for (int i = 0; i < parameters.size(); i++) {
            Parameter parameter = parameters.get(i);
            if (parameter == null) {
                throw new SQLException("Parameter #" + (i+1) + " is not set");
            }
            params.add(parameter.value);
        }
        return params;
    }

    void setParameter(int parameterIndex, Object parameter) throws SQLException {
        setParameter(parameterIndex, new Parameter(parameter));
    }

    void setNullValue(int parameterIndex) throws SQLException {
        setParameter(parameterIndex, NULL_VALUE);
    }

    void setParameter(int parameterIndex, Parameter parameter) throws SQLException {
        if (parameterIndex > parameters.size()) {
            throw new SQLException("Invalid parameter index value: " + parameterIndex);
        }
        parameters.add(parameterIndex - 1, new Parameter(parameter));
    }

    private static class Parameter {
        private final Object value;

        private Parameter(Object value) {
            this.value = value;
        }
    }
}
