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

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcUrlTest {

    @Test
    void shouldReturnTrueForAcceptValidUrl() {
        assertThat(JdbcUrl.acceptsUrl("jdbc:hazelcast://localhost:5701/public")).isTrue();
    }

    @Test
    void shouldReturnFalseForAcceptInvalidUrl() {
        assertThat(JdbcUrl.acceptsUrl("somerandomstring")).isFalse();
    }

    @Test
    void shouldReturnNullIfUrlIsNotValid() {
        assertThat(JdbcUrl.valueOf("somerandomstring", new Properties())).isNull();
    }

    @Test
    void shouldParseUrlToCorrectProperties() {
        JdbcUrl url = JdbcUrl.valueOf("jdbc:hazelcast://localhost:5701/public?prop1=val1&prop2=val2", new Properties());

        assertThat(url).isNotNull();
        assertThat(url.getAuthorities()).contains("localhost:5701");
        assertThat(url.getSchema()).isEqualTo("public");
        assertThat(url.getProperty("prop1")).isEqualTo("val1");

        JdbcUrl urlWithoutPort = JdbcUrl.valueOf("jdbc:hazelcast://clustername/public", new Properties());
        assertThat(urlWithoutPort).isNotNull();
        assertThat(urlWithoutPort.getAuthorities()).contains("clustername");
    }

    @Test
    void shouldDecodeUrlEncodedString() {
        JdbcUrl url = JdbcUrl.valueOf("jdbc:haze%6Ccas%74%3A//localhos%74%3A5701/public", new Properties());
        assertThat(url).isNotNull();
        assertThat(url.getAuthorities()).contains("localhost:5701");
        assertThat(url.getSchema()).isEqualTo("public");
    }
}