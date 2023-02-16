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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

class JdbcUrlTest {

    public static List<String> parameters() {
        return Arrays.asList("hazelcast", "zdih");
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void acceptsUrl_when_validUrl_then_true(String productName) {
        assertThat(JdbcUrl.acceptsUrl("jdbc:" + productName + "://localhost:5701/")).isTrue();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void acceptsUrl_when_validPrefixInvalidUrl_then_true(String productName) {
        assertThat(JdbcUrl.acceptsUrl("jdbc:" + productName + ":foo")).isTrue();
    }

    @Test
    void acceptsUrl_when_invalidPrefix_then_false() {
        assertThat(JdbcUrl.acceptsUrl("somerandomstring")).isFalse();
    }

    @Test
    void constructor_when_invalid_then_throw() {
        assertThatThrownBy(() -> new JdbcUrl("somerandomstring", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void test_propertyParsing(String productName) {
        JdbcUrl url = new JdbcUrl("jdbc:" + productName + "://localhost:5701/?prop1=val1&prop2=val2", new Properties());

        assertThat(url).isNotNull();
        assertThat(url.getAuthorities()).contains("localhost:5701");
        assertThat(url.getProperty("prop1")).isEqualTo("val1");

        JdbcUrl urlWithoutPort = new JdbcUrl("jdbc:" + productName + "://clustername/", new Properties());
        assertThat(urlWithoutPort).isNotNull();
        assertThat(urlWithoutPort.getAuthorities()).contains("clustername");
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void test_propertyParsing_withEscaping(String productName) {
        JdbcUrl url = new JdbcUrl("jdbc:" + productName + "://local%68ost:5701/?a=%26&b%3d=c", new Properties());
        assertThat(url).isNotNull();
        assertThat(url.getAuthorities()).containsExactly("localhost:5701");
        assertThat(url.getProperty("a")).isEqualTo("&");
        assertThat(url.getProperty("b=")).isEqualTo("c");
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void when_sameKeyInUrlAndProperties_then_thatFromUrlTakesPrecedence(String productName) {
        Properties props = new Properties();
        props.setProperty("a", "foo");
        JdbcUrl url = new JdbcUrl("jdbc:" + productName + "://localhost/?a=bar", props);
        assertEquals("bar", url.getProperty("a"));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void when_duplicateKeyInUrl_then_lastOccurrenceUsed(String productName) {
        JdbcUrl url = new JdbcUrl("jdbc:" + productName + "://localhost/?a=foo&a=bar", null);
        assertEquals("bar", url.getProperty("a"));
    }
}
