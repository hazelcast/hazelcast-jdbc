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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

@SuppressFBWarnings("NM_SAME_SIMPLE_NAME_AS_INTERFACE")
public class Driver implements java.sql.Driver {

    static final int VER_MAJOR = 4;
    static final int VER_MINOR = 0;

    private static final Driver INSTANCE = new Driver();
    private static boolean registered;

    static {
        load();
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (url == null) {
            throw new SQLException("URL is null");
        }
        if (!JdbcUrl.acceptsUrl(url)) {
            return null;
        }
        JdbcUrl jdbcUrl;
        try {
            jdbcUrl = new JdbcUrl(url, info);
        } catch (IllegalArgumentException e) {
            // convert to SQLException
            throw new SQLException(e.getMessage(), e);
        }
        return new JdbcConnection(new HazelcastSqlClient(jdbcUrl));
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            throw new SQLException("URL is null");
        }
        return JdbcUrl.acceptsUrl(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return VER_MAJOR;
    }

    @Override
    public int getMinorVersion() {
        return VER_MINOR;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("The driver does not use java.util.logging");
    }

    private static void load() {
        try {
            if (!registered) {
                DriverManager.registerDriver(INSTANCE);
                registered = true;
            }
        } catch (SQLException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }
}
