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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class JdbcUrl {

    private static final Pattern JDBC_URL_PATTERN = Pattern.compile("^jdbc:hazelcast://"
            + "(?<authority>\\S+?(?=[/]))"
            + "/(?<schema>\\S+?)"
            + "(\\?(?<parameters>\\S*))?$");

    private final List<String> authorities;
    private final String schema;
    private final String rawUrl;
    private Properties properties = new Properties();
    private final String rawAuthority;

    private JdbcUrl(String rawAuthority, String schema, String rawUrl) {
        this.rawAuthority = rawAuthority;
        this.authorities = Arrays.asList(rawAuthority.split(","));
        this.schema = schema;
        this.rawUrl = rawUrl;
    }

    public List<String> getAuthorities() {
        return authorities;
    }

    public String getSchema() {
        return schema;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public String getRawUrl() {
        return rawUrl;
    }

    public String getRawAuthority() {
        return rawAuthority;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JdbcUrl jdbcUrl = (JdbcUrl) o;
        return Objects.equals(properties, jdbcUrl.properties) && Objects.equals(rawUrl, jdbcUrl.rawUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(properties, rawUrl);
    }

    private void parseProperties(String parametersString) {
        if (parametersString == null) {
            return;
        }
        for (String parameter : parametersString.split("&")) {
            String[] paramAndValue = parameter.split("=", 2);
            if (paramAndValue.length != 2) {
                continue;
            }
            properties.setProperty(decodeUrl(paramAndValue[0]), decodeUrl(paramAndValue[1]));
        }
    }

    static boolean acceptsUrl(String url) {
        return JDBC_URL_PATTERN.matcher(url).matches();
    }

    static JdbcUrl valueOf(String url, Properties info) {
        Matcher matcher = JDBC_URL_PATTERN.matcher(url);
        if (!matcher.matches()) {
            return null;
        }

        JdbcUrl jdbcUrl = new JdbcUrl(decodeUrl(matcher.group("authority")), decodeUrl(matcher.group("schema")), url);
        if (info != null) {
            jdbcUrl.properties = info;
        }
        jdbcUrl.parseProperties(matcher.group("parameters"));
        return jdbcUrl;
    }

    static JdbcUrl valueOf(String url) {
        return valueOf(url, new Properties());
    }

    private static String decodeUrl(String raw) {
        try {
            return URLDecoder.decode(raw, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException impossible) {
            throw new RuntimeException(impossible);
        }
    }
}
