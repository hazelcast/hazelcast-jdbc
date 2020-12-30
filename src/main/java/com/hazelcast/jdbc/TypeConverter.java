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

import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.converter.Converter;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

final class TypeConverter {

    public static final int MILLIS_IN_SECONDS = 1_000;
    private static final Map<Class<?>, BiFunction<Object, Converter, ?>> CLASS_TO_TYPE_CONVERSION = new HashMap<>();
    private static final Map<Class<?>, QueryDataTypeFamily> CLASS_TO_QUERY_TYPE = new HashMap<>();

    private TypeConverter() {
    }

    static {
        initConverterMapping();
        initTypeMapping();
    }

    private static void initConverterMapping() {
        CLASS_TO_TYPE_CONVERSION.put(Integer.class, (o, c) -> c.asInt(o));
        CLASS_TO_TYPE_CONVERSION.put(Long.class, (o, c) -> c.asBigint(o));
        CLASS_TO_TYPE_CONVERSION.put(Short.class, (o, c) -> c.asSmallint(o));
        CLASS_TO_TYPE_CONVERSION.put(Byte.class, (o, c) -> c.asTinyint(o));
        CLASS_TO_TYPE_CONVERSION.put(Float.class, (o, c) -> c.asReal(o));
        CLASS_TO_TYPE_CONVERSION.put(Double.class, (o, c) -> c.asDouble(o));
        CLASS_TO_TYPE_CONVERSION.put(String.class, (o, c) -> c.asVarchar(o));
        CLASS_TO_TYPE_CONVERSION.put(Boolean.class, (o, c) -> c.asBoolean(o));
        CLASS_TO_TYPE_CONVERSION.put(BigDecimal.class, (o, c) -> c.asDecimal(o));
        CLASS_TO_TYPE_CONVERSION.put(Timestamp.class, TypeConverter::convertToTimestamp);
        CLASS_TO_TYPE_CONVERSION.put(Time.class, TypeConverter::convertToTime);
        CLASS_TO_TYPE_CONVERSION.put(Date.class, TypeConverter::convertToDate);
    }

    private static void initTypeMapping() {
        CLASS_TO_QUERY_TYPE.put(Integer.class, QueryDataTypeFamily.INTEGER);
        CLASS_TO_QUERY_TYPE.put(Long.class, QueryDataTypeFamily.BIGINT);
        CLASS_TO_QUERY_TYPE.put(Short.class, QueryDataTypeFamily.SMALLINT);
        CLASS_TO_QUERY_TYPE.put(Byte.class, QueryDataTypeFamily.TINYINT);
        CLASS_TO_QUERY_TYPE.put(Float.class, QueryDataTypeFamily.REAL);
        CLASS_TO_QUERY_TYPE.put(Double.class, QueryDataTypeFamily.DOUBLE);
        CLASS_TO_QUERY_TYPE.put(String.class, QueryDataTypeFamily.VARCHAR);
        CLASS_TO_QUERY_TYPE.put(Boolean.class, QueryDataTypeFamily.BOOLEAN);
        CLASS_TO_QUERY_TYPE.put(BigDecimal.class, QueryDataTypeFamily.DECIMAL);
        CLASS_TO_QUERY_TYPE.put(Timestamp.class, QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE);
        CLASS_TO_QUERY_TYPE.put(Time.class, QueryDataTypeFamily.TIME);
        CLASS_TO_QUERY_TYPE.put(Date.class, QueryDataTypeFamily.DATE);
    }

    @SuppressWarnings("unchecked")
    static <T> T convertTo(Object object, QueryDataType targetDataType) throws SQLException {
        if (object == null) {
            return null;
        }
        try {
            return (T) targetDataType.convert(object);
        } catch (Exception e) {
            throw new SQLException("Cannot convert '" + object + "' of type "
                    + object.getClass().getSimpleName()
                    + " to " + targetDataType.getConverter().getNormalizedValueClass().getSimpleName(), e);
        }
    }

    private static Timestamp convertToTimestamp(Object o, Converter c) {
        return new Timestamp(c.asTimestampWithTimezone(o).toEpochSecond() * MILLIS_IN_SECONDS);
    }

    private static Time convertToTime(Object o, Converter c) {
        return new Time(c.asTimestamp(o).toEpochSecond(ZoneOffset.UTC) * MILLIS_IN_SECONDS);
    }

    private static Date convertToDate(Object o, Converter c) {
        return new Date(c.asDate(o).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * MILLIS_IN_SECONDS);
    }
}
