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

import com.hazelcast.sql.SqlColumnType;

import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("checkstyle:ExecutableStatementCount")
final class TypeUtil {
    private static final Map<SqlColumnType, String> SQL_TYPES_NAME_MAPPING = new HashMap<>();
    private static final Map<SqlColumnType, Integer> SQL_TYPES_MAPPING = new HashMap<>();
    private static final Map<SqlColumnType, SqlTypeInfo> SQL_TYPES_INFO = new HashMap<>();
    private static final Map<String, SqlColumnType> QDT_NAMES_TO_SQL_TYPES_MAPPING = new HashMap<>();
    private static final Set<SqlColumnType> NUMERIC_TYPES = new HashSet<>();

    private TypeUtil() { }

    static {
        SQL_TYPES_NAME_MAPPING.put(SqlColumnType.BIGINT, "BIGINT");
        SQL_TYPES_NAME_MAPPING.put(SqlColumnType.VARCHAR, "VARCHAR");
        SQL_TYPES_NAME_MAPPING.put(SqlColumnType.BOOLEAN, "BOOLEAN");
        SQL_TYPES_NAME_MAPPING.put(SqlColumnType.TINYINT, "TINYINT");
        SQL_TYPES_NAME_MAPPING.put(SqlColumnType.SMALLINT, "SMALLINT");
        SQL_TYPES_NAME_MAPPING.put(SqlColumnType.INTEGER, "INT");
        SQL_TYPES_NAME_MAPPING.put(SqlColumnType.DECIMAL, "DECIMAL");
        SQL_TYPES_NAME_MAPPING.put(SqlColumnType.REAL, "REAL");
        SQL_TYPES_NAME_MAPPING.put(SqlColumnType.DOUBLE, "DOUBLE");
        SQL_TYPES_NAME_MAPPING.put(SqlColumnType.TIME, "TIME");
        SQL_TYPES_NAME_MAPPING.put(SqlColumnType.DATE, "DATE");
        SQL_TYPES_NAME_MAPPING.put(SqlColumnType.TIMESTAMP, "TIMESTAMP");
        SQL_TYPES_NAME_MAPPING.put(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, "TIMESTAMP WITH TIME ZONE");
        SQL_TYPES_NAME_MAPPING.put(SqlColumnType.OBJECT, "OBJECT");
        SQL_TYPES_NAME_MAPPING.put(SqlColumnType.JSON, "JSON");

        SQL_TYPES_MAPPING.put(SqlColumnType.VARCHAR, Types.VARCHAR);
        SQL_TYPES_MAPPING.put(SqlColumnType.BOOLEAN, Types.BOOLEAN);
        SQL_TYPES_MAPPING.put(SqlColumnType.TINYINT, Types.TINYINT);
        SQL_TYPES_MAPPING.put(SqlColumnType.SMALLINT, Types.SMALLINT);
        SQL_TYPES_MAPPING.put(SqlColumnType.INTEGER, Types.INTEGER);
        SQL_TYPES_MAPPING.put(SqlColumnType.BIGINT, Types.BIGINT);
        SQL_TYPES_MAPPING.put(SqlColumnType.DECIMAL, Types.DECIMAL);
        SQL_TYPES_MAPPING.put(SqlColumnType.REAL, Types.REAL);
        SQL_TYPES_MAPPING.put(SqlColumnType.DOUBLE, Types.DOUBLE);
        SQL_TYPES_MAPPING.put(SqlColumnType.DATE, Types.DATE);
        SQL_TYPES_MAPPING.put(SqlColumnType.TIME, Types.TIME);
        SQL_TYPES_MAPPING.put(SqlColumnType.TIMESTAMP, Types.TIMESTAMP);
        SQL_TYPES_MAPPING.put(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, Types.TIMESTAMP_WITH_TIMEZONE);
        SQL_TYPES_MAPPING.put(SqlColumnType.OBJECT, Types.JAVA_OBJECT);
        SQL_TYPES_MAPPING.put(SqlColumnType.NULL, Types.NULL);
        SQL_TYPES_MAPPING.put(SqlColumnType.JSON, Types.OTHER);

        SQL_TYPES_INFO.put(SqlColumnType.INTEGER, new SqlTypeInfo(Constants.INTEGER_DISPLAY_SIZE,
                Constants.INTEGER_DISPLAY_SIZE, Constants.ZERO, true));
        SQL_TYPES_INFO.put(SqlColumnType.BIGINT, new SqlTypeInfo(Constants.BIGINT_DISPLAY_SIZE,
                Constants.BIGINT_DISPLAY_SIZE, Constants.ZERO, true));
        SQL_TYPES_INFO.put(SqlColumnType.TINYINT, new SqlTypeInfo(Constants.TINYINT_DISPLAY_SIZE, Constants.TINYINT_PRECISION,
                Constants.ZERO, true));
        SQL_TYPES_INFO.put(SqlColumnType.SMALLINT, new SqlTypeInfo(Constants.SMALLINT_DISPLAY_SIZE,
                Constants.SMALLINT_PRECISION, Constants.ZERO, true));

        SQL_TYPES_INFO.put(SqlColumnType.REAL, new SqlTypeInfo(Constants.REAL_DISPLAY_SIZE,
                Constants.REAL_PRECISION, Constants.REAL_PRECISION, true));
        SQL_TYPES_INFO.put(SqlColumnType.DOUBLE, new SqlTypeInfo(Constants.DOUBLE_DISPLAY_SIZE, Constants.DOUBLE_PRECISION,
                Constants.DOUBLE_PRECISION, true));
        SQL_TYPES_INFO.put(SqlColumnType.DECIMAL, new SqlTypeInfo(Constants.DECIMAL_DISPLAY_SIZE, Constants.DECIMAL_PRECISION,
                Constants.DECIMAL_PRECISION, true));

        SQL_TYPES_INFO.put(SqlColumnType.VARCHAR, new SqlTypeInfo(Constants.STRING_DISPLAY_SIZE,
                Constants.MAX_STRING_LENGTH, Constants.ZERO, false));
        SQL_TYPES_INFO.put(SqlColumnType.BOOLEAN, new SqlTypeInfo(Constants.BOOLEAN_DISPLAY_SIZE, Constants.BOOLEAN_PRECISION,
                Constants.ZERO, false));

        SQL_TYPES_INFO.put(SqlColumnType.NULL, new SqlTypeInfo(Constants.NULL_DISPLAY_SIZE,
                Constants.BOOLEAN_PRECISION, Constants.ZERO, false));
        SQL_TYPES_INFO.put(SqlColumnType.OBJECT, new SqlTypeInfo(Constants.MAX_STRING_LENGTH,
                Constants.MAX_STRING_LENGTH, Constants.ZERO, false));
        SQL_TYPES_INFO.put(SqlColumnType.JSON, new SqlTypeInfo(Constants.MAX_STRING_LENGTH,
                Constants.MAX_STRING_LENGTH, Constants.ZERO, false));

        SQL_TYPES_INFO.put(SqlColumnType.DATE, new SqlTypeInfo(Constants.DATE_DISPLAY_SIZE,
                Constants.DATE_DISPLAY_SIZE, Constants.ZERO, false));
        SQL_TYPES_INFO.put(SqlColumnType.TIME, new SqlTypeInfo(Constants.TIME_DISPLAY_SIZE,
                Constants.TIME_DISPLAY_SIZE, Constants.ZERO, false));
        SQL_TYPES_INFO.put(SqlColumnType.TIMESTAMP, new SqlTypeInfo(Constants.TIMESTAMP_DISPLAY_SIZE,
                Constants.TIMESTAMP_DISPLAY_SIZE, Constants.ZERO, false));
        SQL_TYPES_INFO.put(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, new SqlTypeInfo(Constants.TIMESTAMP_DISPLAY_SIZE,
                Constants.TIMESTAMP_DISPLAY_SIZE, Constants.ZERO, false));

        QDT_NAMES_TO_SQL_TYPES_MAPPING.put("BIGINT", SqlColumnType.BIGINT);
        QDT_NAMES_TO_SQL_TYPES_MAPPING.put("VARCHAR", SqlColumnType.VARCHAR);
        QDT_NAMES_TO_SQL_TYPES_MAPPING.put("BOOLEAN", SqlColumnType.BOOLEAN);
        QDT_NAMES_TO_SQL_TYPES_MAPPING.put("TINYINT", SqlColumnType.TINYINT);
        QDT_NAMES_TO_SQL_TYPES_MAPPING.put("SMALLINT", SqlColumnType.SMALLINT);
        QDT_NAMES_TO_SQL_TYPES_MAPPING.put("INTEGER", SqlColumnType.INTEGER);
        QDT_NAMES_TO_SQL_TYPES_MAPPING.put("DECIMAL", SqlColumnType.DECIMAL);
        QDT_NAMES_TO_SQL_TYPES_MAPPING.put("REAL", SqlColumnType.REAL);
        QDT_NAMES_TO_SQL_TYPES_MAPPING.put("DOUBLE", SqlColumnType.DOUBLE);
        QDT_NAMES_TO_SQL_TYPES_MAPPING.put("TIME", SqlColumnType.TIME);
        QDT_NAMES_TO_SQL_TYPES_MAPPING.put("DATE", SqlColumnType.DATE);
        QDT_NAMES_TO_SQL_TYPES_MAPPING.put("TIMESTAMP", SqlColumnType.TIMESTAMP);
        QDT_NAMES_TO_SQL_TYPES_MAPPING.put("TIMESTAMP_WITH_TIME_ZONE", SqlColumnType.TIMESTAMP_WITH_TIME_ZONE);
        QDT_NAMES_TO_SQL_TYPES_MAPPING.put("OBJECT", SqlColumnType.OBJECT);
        QDT_NAMES_TO_SQL_TYPES_MAPPING.put("JSON", SqlColumnType.JSON);

        NUMERIC_TYPES.add(SqlColumnType.TINYINT);
        NUMERIC_TYPES.add(SqlColumnType.SMALLINT);
        NUMERIC_TYPES.add(SqlColumnType.BIGINT);
        NUMERIC_TYPES.add(SqlColumnType.DECIMAL);
        NUMERIC_TYPES.add(SqlColumnType.REAL);
        NUMERIC_TYPES.add(SqlColumnType.DOUBLE);
    }

    public static String getName(SqlColumnType columnType) {
        return SQL_TYPES_NAME_MAPPING.get(columnType);
    }

    public static SqlColumnType getTypeByQDTName(String name) {
        return QDT_NAMES_TO_SQL_TYPES_MAPPING.getOrDefault(name, SqlColumnType.OBJECT);
    }

    public static SqlTypeInfo getTypeInfo(final SqlColumnType columnType) {
        return SQL_TYPES_INFO.get(columnType);
    }

    public static Integer getJdbcType(final SqlColumnType columnType) {
        return SQL_TYPES_MAPPING.get(columnType);
    }

    public static boolean isNumeric(final SqlColumnType sqlColumnType) {
        return NUMERIC_TYPES.contains(sqlColumnType);
    }

    public static final class SqlTypeInfo {
        private final Integer displaySize;
        private final Integer precision;
        private final Integer scale;
        private final Boolean signed;

        private SqlTypeInfo(Integer displaySize, Integer precision, Integer scale, final Boolean signed) {
            this.displaySize = displaySize;
            this.precision = precision;
            this.scale = scale;
            this.signed = signed;
        }

        public Integer getDisplaySize() {
            return displaySize;
        }

        public Integer getPrecision() {
            return precision;
        }

        public Integer getScale() {
            return scale;
        }

        public Boolean isSigned() {
            return signed;
        }
    }
}
