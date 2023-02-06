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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.sql.SqlResult;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;

public class JdbcTestSupport {

    /**
     * Create an IMap mapping with the given {@code name} that uses
     * java serialization for both key and value with the given classes.
     */
    public static void createMapping(HazelcastInstance instance, String name, Class<?> keyClass, Class<?> valueClass) {
        try (SqlResult result = instance.getSql().execute("CREATE OR REPLACE MAPPING \"" + name + "\" TYPE " + IMapSqlConnector.TYPE_NAME + "\n"
                + "OPTIONS (\n"
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                + ", '" + OPTION_KEY_CLASS + "'='" + keyClass.getName() + "'\n"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                + ", '" + OPTION_VALUE_CLASS + "'='" + valueClass.getName() + "'\n"
                + ")"
        )) {
            assertThat(result.updateCount()).isEqualTo(0);
        }
    }
}
