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
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;
import com.hazelcast.sql.impl.type.converter.Converters;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.function.Supplier;

final class TypeConverter {

    public static final int MILLIS_IN_SECONDS = 1_000;

    private TypeConverter() {
    }

    @SuppressWarnings("unchecked")
    static <T> T convertTo(Object object, QueryDataType targetDataType) throws SQLException {
        return convertAs(
                object, () -> (T) targetDataType.convert(object), targetDataType.getConverter().getNormalizedValueClass());
    }

    @SuppressWarnings("unchecked")
    static <T> T convertTo(Object object, Class<T> clazz) throws SQLException {
        if (clazz == Timestamp.class) {
            return (T) convertToTimestamp(object);
        }
        if (clazz == Time.class) {
            return (T) convertToTime(object);
        }
        if (clazz == Date.class) {
            return (T) convertToDate(object);
        }
        QueryDataType queryDataType = QueryDataTypeUtils.resolveTypeForClass(clazz);
        return convertAs(object, () -> (T) queryDataType.convert(object), clazz);
    }

    static Timestamp convertToTimestamp(Object object) throws SQLException {
        return convertAs(object, () -> new Timestamp(toMillis(
                Converters.getConverter(object.getClass()).asTimestampWithTimezone(object).toEpochSecond())),
                Timestamp.class);
    }

    static Time convertToTime(Object object) throws SQLException {
        return convertAs(object, () -> new Time(toMillis(
                Converters.getConverter(object.getClass()).asTimestamp(object).toEpochSecond(ZoneOffset.UTC))),
                Time.class);
    }

    static Date convertToDate(Object object)  throws SQLException  {
        return convertAs(object, () -> new Date(toMillis(
                Converters.getConverter(object.getClass()).asDate(object).atStartOfDay().toEpochSecond(ZoneOffset.UTC))),
                Date.class);
    }

    private static <T> T convertAs(Object object, Supplier<T> supplier, Class<?> targetClass) throws SQLException {
        if (object == null) {
            return null;
        }
        try {
            return supplier.get();
        } catch (Exception e) {
            throw new SQLException("Cannot convert '" + object + "' of type "
                    + object.getClass().getSimpleName()
                    + " to " + targetClass.getSimpleName(), e);
        }
    }

    private static long toMillis(long seconds) {
        return seconds * MILLIS_IN_SECONDS;
    }
}
