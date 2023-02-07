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

import java.io.Serializable;
import java.math.BigDecimal;

public class ValuesWrapper implements Serializable {
    private static final long serialVersionUID = 5855947779681687264L;

    private String string;
    private Boolean aBoolean;
    private Integer integer;
    private Long aLong;
    private BigDecimal bigDecimal;
    private Float real;
    private Byte tinyInt;
    private Short smallInt;
    private Double aDouble;
    private Character character;

    ValuesWrapper(String string) {
        this.string = string;
    }

    ValuesWrapper(Boolean aBoolean) {
        this.aBoolean = aBoolean;
    }

    ValuesWrapper(Integer integer) {
        this.integer = integer;
    }

    ValuesWrapper(Long aLong) {
        this.aLong = aLong;
    }

    ValuesWrapper(BigDecimal bigDecimal) {
        this.bigDecimal = bigDecimal;
    }

    ValuesWrapper(Float real) {
        this.real = real;
    }

    ValuesWrapper(Byte tinyInt) {
        this.tinyInt = tinyInt;
    }

    ValuesWrapper(Short smallInt) {
        this.smallInt = smallInt;
    }

    ValuesWrapper(Double aDouble) {
        this.aDouble = aDouble;
    }

    ValuesWrapper(Character character) {
        this.character = character;
    }

    public Double getDouble() {
        return aDouble;
    }

    public Short getSmallInt() {
        return smallInt;
    }

    public Byte getTinyInt() {
        return tinyInt;
    }

    public Float getReal() {
        return real;
    }

    public String getString() {
        return string;
    }

    public Boolean getBoolean() {
        return aBoolean;
    }

    public Integer getInteger() {
        return integer;
    }

    public Long getLong() {
        return aLong;
    }

    public BigDecimal getBigDecimal() {
        return bigDecimal;
    }

    public Character getCharacter() {
        return character;
    }
}
