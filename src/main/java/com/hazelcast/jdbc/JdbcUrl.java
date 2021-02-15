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
    private String rawAuthority;

    private JdbcUrl(List<String> authorities, String schema, String rawUrl) {
        this.authorities = authorities;
        this.schema = schema;
        this.rawUrl = rawUrl;
    }

    public List<String> getAuthorities() {
        return authorities;
    }

    public String getSchema() {
        return schema;
    }

    public Properties getProperties() {
        return properties;
    }

    public String getRawUrl() {
        return rawUrl;
    }

    public String getRawAuthority() {
        return rawAuthority;
    }

    private void parseProperties(String parametersString) {
        if (parametersString == null) {
            return;
        }
        for (String parameter : parametersString.split("&")) {
            String[] paramAndValue = parameter.split("=");
            if (paramAndValue.length != 2) {
                continue;
            }
            properties.setProperty(paramAndValue[0], paramAndValue[1]);
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

        String rawAuthority = decodeUrl(matcher.group("authority"));
        JdbcUrl jdbcUrl = new JdbcUrl(Arrays.asList(rawAuthority.split(",")), matcher.group("schema"), url);
        jdbcUrl.rawAuthority = rawAuthority;
        if (info != null) {
            jdbcUrl.properties = info;
        }
        jdbcUrl.parseProperties(matcher.group("parameters"));
        return jdbcUrl;
    }

    static JdbcUrl valueOf(String url) {
        return valueOf(url, new Properties());
    }

    static String decodeUrl(String raw) {
        try {
            return URLDecoder.decode(raw, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException impossible) {
            throw new IllegalArgumentException(impossible);
        }
    }

}
