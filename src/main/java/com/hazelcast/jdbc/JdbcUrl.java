package com.hazelcast.jdbc;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class JdbcUrl {

    private static final Pattern JDBC_URL_PATTERN = Pattern.compile("^jdbc:hazelcast://" +
            "(?<host>\\S+?(?=[:/]))(:(?<port>\\d+))?" +
            "/(?<schema>\\S+?)" +
            "(\\?(?<parameters>\\S*))?$");

    private String host;
    private int port = -1;
    private String schema;
    private Properties properties = new Properties();

    private JdbcUrl(String host, String schema) {
        this.host = host;
        this.schema = schema;
    }

    public String getAuthority() {
        if (port == -1) {
            return host;
        }
        return host + ":" + port;
    }

    public String getSchema() {
        return schema;
    }

    public Properties getProperties() {
        return properties;
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
        JdbcUrl jdbcUrl = new JdbcUrl(matcher.group("host"), matcher.group("schema"));
        if (info != null) {
            jdbcUrl.properties = info;
        }
        String port = matcher.group("port");
        if (port != null) {
            jdbcUrl.port = Integer.parseInt(port);
        }
        jdbcUrl.parseProperties(matcher.group("parameters"));
        return jdbcUrl;
    }
}
