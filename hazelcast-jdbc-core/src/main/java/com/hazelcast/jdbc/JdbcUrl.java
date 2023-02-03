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
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class JdbcUrl {

    private static final String PREFIX = "jdbc:hazelcast:";
    private static final Pattern JDBC_URL_PATTERN = Pattern.compile(PREFIX + "//"
            + "(?<authority>\\S+?)/?"
            + "(\\?(?<parameters>\\S*))?");

    private final List<String> authorities;
    private final String rawUrl;
    private final Properties properties = new Properties();
    private final String rawAuthority;

    JdbcUrl(String url, Properties properties) {
        Matcher matcher = JDBC_URL_PATTERN.matcher(url);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("The URL doesn't match the structure - "
                    + "jdbc:hazelcast://host:port[,host2:port...]/[?prop1=value1&...]");
        }

        this.rawAuthority = decodeUrl(matcher.group("authority"));
        this.authorities = Arrays.asList(rawAuthority.split(","));
        this.rawUrl = url;

        if (properties != null) {
            properties.forEach(this.properties::put);
        }
        String parametersString = matcher.group("parameters");
        if (parametersString != null) {
            for (String parameter : parametersString.split("&")) {
                String[] paramAndValue = parameter.split("=", 2);
                if (paramAndValue.length != 2) {
                    paramAndValue = new String[]{paramAndValue[0], ""};
                }
                this.properties.setProperty(
                    decodeUrl(paramAndValue[0]), decodeUrl(paramAndValue[1]));
            }
        }
    }

    public List<String> getAuthorities() {
        return authorities;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public Map<String, String> getProperties() {
        return (Map) properties;
    }

    public String getRawUrl() {
        return rawUrl;
    }

    public String getRawAuthority() {
        return rawAuthority;
    }

    static boolean acceptsUrl(String url) {
        return url.startsWith(PREFIX);
    }

    private static String decodeUrl(String raw) {
        try {
            return URLDecoder.decode(raw, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException impossible) {
            throw new RuntimeException(impossible);
        }
    }
}
