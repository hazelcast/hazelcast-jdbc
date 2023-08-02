package com.hazelcast.smoke.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class SmokeTestDriver {

    private static final Logger logger = Logger.getLogger(SmokeTestDriver.class.getName());

    public static void main(String[] args) throws SQLException {

        if (args.length < 2) {
            logger.severe("Usage: 'java "
                    + SmokeTestDriver.class.getName()
                    + " <port> <clusterName>");
            throw new IllegalArgumentException("Too few arguments");
        }

        String hzPort = args[0];
        String clusterName = args[1];
        try (Connection con = DriverManager.getConnection(
                "jdbc:hazelcast://localhost:" + hzPort + "/?clusterName=" + clusterName)) {
            logger.info("Database: " + con.getMetaData().getDatabaseProductName()
                    + ": " + con.getMetaData().getDatabaseProductVersion());

            try (Statement s = con.createStatement()) {
                s.execute("CREATE OR REPLACE MAPPING map "
                        + "TYPE IMap OPTIONS("
                        + "  'keyFormat'='integer', "
                        + "  'valueFormat'='varchar'"
                        + ")");

                int insertCount = s.executeUpdate(
                        "INSERT INTO map "
                        + "SELECT v, 'name-' || v "
                        + "FROM TABLE(generate_series(0, 9))"
                );
                assert insertCount == 0;

                ResultSet rs = s.executeQuery(
                        "SELECT __key, this "
                        + "FROM map "
                        + "ORDER BY __key"
                );
                int rowCount = 0;
                while (rs.next()) {
                    String row = String.format("__key: %d value: %s",
                            rs.getInt("__key"), rs.getString("this"));
                    logger.info(row);
                    rowCount++;
                }

                assert rowCount == 10;

                int deleteCount = s.executeUpdate("DELETE FROM map");
                assert deleteCount == insertCount;

                s.execute("DROP MAPPING map");
            }
        }
    }
}