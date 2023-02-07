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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Wraps the list of parameters used for {@link java.sql.PreparedStatement}.
 * Ensures that all the parameters are set at a correct {@code parameterIndex}.
 */
class ParameterList {

    private static final Parameter NULL_VALUE = new Parameter(null);

    private final List<Parameter> parameters = new ArrayList<>(0);

    /**
     * @return the list of the parameter values
     * @throws SQLException if any of the parameters is not set
     */
    List<Object> asParameters() throws SQLException {
        if (parameters.isEmpty()) {
            return Collections.emptyList();
        }
        List<Object> params = new ArrayList<>(parameters.size());
        for (int i = 0; i < parameters.size(); i++) {
            Parameter parameter = parameters.get(i);
            if (parameter == null) {
                throw new SQLException("Parameter #" + (i + 1) + " is not set");
            }
            params.add(parameter.value);
        }
        return params;
    }

    /**
     * Sets the parameter value for the given {@code parameterIndex} of any type
     *
     * @param parameterIndex first parameter is 1, second parameter is 2...
     * @param parameter parameter value
     */
    void setParameter(int parameterIndex, Object parameter) {
        setParameter(parameterIndex, new Parameter(parameter));
    }

    /**
     * Sets the {@literal null} for the given {@code parameterIndex} of any type
     * @param parameterIndex first parameter is 1, second parameter is 2...
     */
    void setNullValue(int parameterIndex) {
        setParameter(parameterIndex, NULL_VALUE);
    }

    /**
     * Sets the parameter value for the given {@code parameterIndex} of any type
     * @param parameterIndex first parameter is 1, second parameter is 2...
     * @param parameter {@code Parameter} wrapper for the value
     */
    private void setParameter(int parameterIndex, Parameter parameter) {
        while (parameterIndex > parameters.size()) {
            parameters.add(null);
        }
        parameters.set(parameterIndex - 1, parameter);
    }

    private static final class Parameter {
        private final Object value;

        private Parameter(Object value) {
            this.value = value;
        }
    }
}
