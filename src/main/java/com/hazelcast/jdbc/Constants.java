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

/**
 * Fixed values that are used internally in the whole Driver code
 */
final class Constants {

    /**
     * The display size for INTEGER type
     */
    static final int INTEGER_DISPLAY_SIZE = 11;

    /**
     * The display size for BIGINT type
     */
    static final int BIGINT_DISPLAY_SIZE = 20;

    /**
     * The display size for BOOLEAN type
     */
    static final int BOOLEAN_DISPLAY_SIZE = 5;

    /**
     * The precision for BOOLEAN type
     */
    static final int BOOLEAN_PRECISION = 1;

    /**
     * The display size for TINYINT type
     */
    static final int TINYINT_DISPLAY_SIZE = 4;

    /**
     * The precision for TINYINT type
     */
    static final int TINYINT_PRECISION = 8;

    /**
     * The display size for SMALLINT type
     */
    static final int SMALLINT_DISPLAY_SIZE = 6;

    /**
     * The precision for SMALLINT type
     */
    static final int SMALLINT_PRECISION = 16;

    /**
     * The display size for REAL type
     */
    static final int REAL_DISPLAY_SIZE = 15;

    /**
     * The precision for REAL type
     */
    static final int REAL_PRECISION = 24;

    /**
     * The display size for DECIMAL type
     */
    static final int DECIMAL_DISPLAY_SIZE = 100_000;

    /**
     * The precision for DECIMAL type
     */
    static final int DECIMAL_PRECISION = 38;

    /**
     * The display size for DOUBLE type
     */
    static final int DOUBLE_DISPLAY_SIZE = 24;

    /**
     * The precision for DOUBLE type
     */
    static final int DOUBLE_PRECISION = 53;

    /**
     * The display size for null value
     */
    static final int NULL_DISPLAY_SIZE = 53;

    /**
     * The display size for DATE type
     */
    static final int DATE_DISPLAY_SIZE = 10;

    /**
     * The display size for TIME type
     */
    static final int TIME_DISPLAY_SIZE = 8;


    /**
     * The max display size for TIMESTAMP and TIMESTAMP_WITH_TIMEZONE types
     */
    static final int TIMESTAMP_DISPLAY_SIZE = 25;

    /**
     * The maximum allowed length for character string and other data types based on it
     */
    static final int MAX_STRING_LENGTH = 2000;

    static final int ZERO = 0;

    private Constants() {
        // utility class
    }
}
