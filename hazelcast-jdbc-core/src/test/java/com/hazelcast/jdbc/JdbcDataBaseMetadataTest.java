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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.jdbc.JdbcTestSupport.createMapping;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

class JdbcDataBaseMetadataTest {

    private static final String JDBC_HAZELCAST_LOCALHOST = "jdbc:hazelcast://localhost:5701/";
    private static Connection connection;
    private static DatabaseMetaData dbMetaData;

    @BeforeAll
    public static void setUp() throws SQLException {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        IMap<Integer, Person> empMap = member.getMap("emp");
        IMap<Integer, String> deptMap = member.getMap("dept");
        createMapping(member, empMap.getName(), int.class, Person.class);
        createMapping(member, deptMap.getName(), int.class, String.class);
        member.getSql().execute("create view emp_dept_view as select * from emp join dept on emp.age=dept.__key");
        connection = DriverManager.getConnection(JDBC_HAZELCAST_LOCALHOST);
        dbMetaData = connection.getMetaData();
    }

    @AfterAll
    public static void tearDown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void test_getTables() throws SQLException {
        List<List<Object>> allTables = asList(
                asList("hazelcast", "public", "dept", "MAPPING", null, null, null, null, null, null),
                asList("hazelcast", "public", "emp", "MAPPING", null, null, null, null, null, null),
                asList("hazelcast", "public", "emp_dept_view", "VIEW", null, null, null, null, null, null));

        // all tables
        assertsResultsExactly(
                allTables,
                dbMetaData.getTables(null, null, null, null));
        assertsResultsExactly(
                allTables,
                dbMetaData.getTables("%", "%", "%", new String[]{"MAPPING", "VIEW"}));
        // all mappings
        assertsResultsExactly(
                asList(asList("hazelcast", "public", "dept", "MAPPING", null, null, null, null, null, null),
                        asList("hazelcast", "public", "emp", "MAPPING", null, null, null, null, null, null)),
                dbMetaData.getTables(null, null, null, new String[]{"MAPPING"}));
        // all views
        assertsResultsExactly(
                singletonList(asList("hazelcast", "public", "emp_dept_view", "VIEW", null, null, null, null, null, null)),
                dbMetaData.getTables(null, null, null, new String[]{"VIEW"}));
        // one catalog
        assertsResultsExactly(
                allTables,
                dbMetaData.getTables("hazelcast", null, null, null));
        assertsResultsExactly(
                emptyList(),
                dbMetaData.getTables("foo", null, null, null));
        // one schema
        assertsResultsExactly(
                allTables,
                dbMetaData.getTables(null, "public", null, null));
        assertsResultsExactly(
                emptyList(),
                dbMetaData.getTables(null, "foo", null, null));
        // one table
        assertsResultsExactly(
                singletonList(asList("hazelcast", "public", "emp", "MAPPING", null, null, null, null, null, null)),
                dbMetaData.getTables(null, null, "emp", null));
    }

    @Test
    public void test_getColumns() throws SQLException {
        List<List<Object>> allColumns = asList(
                asList("hazelcast", "public", "dept", "__key", Types.INTEGER, "INTEGER", 11, null, null, 10,
                        DatabaseMetaData.columnNullable, null, null, null, null, null, 1, "YES", null, null, null, null, "", ""),
                asList("hazelcast", "public", "dept", "this", Types.VARCHAR, "VARCHAR", Integer.MAX_VALUE, null, null, 10,
                        DatabaseMetaData.columnNullable, null, null, null, null, Integer.MAX_VALUE, 2, "YES", null, null, null, null, "", ""),

                asList("hazelcast", "public", "emp", "__key", Types.INTEGER, "INTEGER", 11, null, null, 10,
                        DatabaseMetaData.columnNullable, null, null, null, null, null, 1,
                        "YES", null, null, null, null, "", ""),
                asList("hazelcast", "public", "emp", "age", Types.INTEGER, "INTEGER", 11, null, null, 10,
                        DatabaseMetaData.columnNullable, null, null, null, null, null, 2,
                        "YES", null, null, null, null, "", ""),
                asList("hazelcast", "public", "emp", "name", Types.VARCHAR, "VARCHAR", Integer.MAX_VALUE, null, null, 10,
                        DatabaseMetaData.columnNullable, null, null, null, null, Integer.MAX_VALUE, 3,
                        "YES", null, null, null, null, "", ""),

                asList("hazelcast", "public", "emp_dept_view", "__key", Types.INTEGER, "INTEGER", 11, null, null, 10,
                        DatabaseMetaData.columnNullable, null, null, null, null, null, 1,
                        "YES", null, null, null, null, "", ""),
                asList("hazelcast", "public", "emp_dept_view", "age", Types.INTEGER, "INTEGER", 11, null, null, 10,
                        DatabaseMetaData.columnNullable, null, null, null, null, null, 2,
                        "YES", null, null, null, null, "", ""),
                asList("hazelcast", "public", "emp_dept_view", "name", Types.VARCHAR, "VARCHAR", Integer.MAX_VALUE, null, null, 10,
                        DatabaseMetaData.columnNullable, null, null, null, null, Integer.MAX_VALUE, 3,
                        "YES", null, null, null, null, "", ""),
                asList("hazelcast", "public", "emp_dept_view", "__key0", Types.INTEGER, "INTEGER", 11, null, null, 10,
                        DatabaseMetaData.columnNullable, null, null, null, null, null, 4,
                        "YES", null, null, null, null, "", ""),
                asList("hazelcast", "public", "emp_dept_view", "this", Types.VARCHAR, "VARCHAR", Integer.MAX_VALUE, null, null, 10,
                        DatabaseMetaData.columnNullable, null, null, null, null, Integer.MAX_VALUE, 5,
                        "YES", null, null, null, null, "", "")
        );

        // all columns
        assertsResultsExactly(
                allColumns,
                dbMetaData.getColumns(null, null, null, null));
        // one table
        assertsResultsExactly(
                allColumns.stream().filter(c -> c.get(2).equals("dept")).collect(Collectors.toList()),
                dbMetaData.getColumns(null, null, "dept", null));
        assertsResultsExactly(
                allColumns.stream().filter(c -> c.get(2).equals("dept")).collect(Collectors.toList()),
                dbMetaData.getColumns(null, null, "dept%", null));
        // a column in any table
        assertsResultsExactly(
                allColumns.stream().filter(c -> c.get(3).equals("__key")).collect(Collectors.toList()),
                dbMetaData.getColumns(null, null, null, "__key"));
        // a column prefix in any table
        assertsResultsExactly(
                allColumns.stream().filter(c -> ((String) c.get(3)).startsWith("__key")).collect(Collectors.toList()),
                dbMetaData.getColumns(null, null, null, "__key%"));
        // catalog
        assertsResultsExactly(
                allColumns.stream().filter(c -> c.get(0).equals("hazelcast")).collect(Collectors.toList()),
                dbMetaData.getColumns("hazelcast", null, null, null));
        assertsResultsExactly(
                allColumns.stream().filter(c -> c.get(0).equals("hazelcast")).collect(Collectors.toList()),
                dbMetaData.getColumns("hazelcast%", null, null, null));
        // schema
        assertsResultsExactly(
                allColumns.stream().filter(c -> c.get(1).equals("public")).collect(Collectors.toList()),
                dbMetaData.getColumns(null, "public", null, null));
        assertsResultsExactly(
                allColumns.stream().filter(c -> c.get(1).equals("public")).collect(Collectors.toList()),
                dbMetaData.getColumns(null, "public%", null, null));
        // all conditions at once
        assertsResultsExactly(
                allColumns,
                dbMetaData.getColumns("%", "%", "%", "%"));
        assertsResultsExactly(
                allColumns.stream().filter(c -> c.get(2).equals("emp") && c.get(3).equals("__key")).collect(Collectors.toList()),
                dbMetaData.getColumns("hazelcast", "public", "emp", "__key"));
    }

    @Test
    public void test_getProcedures() throws SQLException {
        assertEmptyResultSet(9,
                dbMetaData.getProcedures(null, null, null));
    }

    @Test
    public void test_getProcedureColumns() throws SQLException {
        assertEmptyResultSet(20,
                dbMetaData.getProcedureColumns(null, null, null, null));
    }

    @Test
    public void test_getCatalogs() throws SQLException {
        assertsResultsExactly(singletonList(singletonList("hazelcast")),
                dbMetaData.getCatalogs());
    }

    @Test
    public void test_getTableTypes() throws SQLException {
        assertsResultsExactly(asList(
                        singletonList("MAPPING"),
                        singletonList("VIEW")),
                dbMetaData.getTableTypes());
    }

    @Test
    public void test_getColumnPrivileges() throws SQLException {
        assertEmptyResultSet(8,
                dbMetaData.getColumnPrivileges(null, null, null, null));
    }

    @Test
    public void test_getTablePrivileges() throws SQLException {
        assertEmptyResultSet(7,
                dbMetaData.getTablePrivileges(null, null, null));
    }

    @Test
    public void test_getBestRowIdentifier() throws SQLException {
        assertEmptyResultSet(8,
                dbMetaData.getBestRowIdentifier(null, null, null, 0, true));
    }

    @Test
    public void test_getVersionColumns() throws SQLException {
        assertEmptyResultSet(8,
                dbMetaData.getVersionColumns(null, null, null));
    }

    @Test
    public void test_getPrimaryKeys() throws SQLException {
        assertEmptyResultSet(6,
                dbMetaData.getPrimaryKeys(null, null, null));
    }

    @Test
    public void test_getImportedKeys() throws SQLException {
        assertEmptyResultSet(14,
                dbMetaData.getImportedKeys(null, null, null));
    }

    @Test
    public void test_getExportedKeys() throws SQLException {
        assertEmptyResultSet(14,
                dbMetaData.getExportedKeys(null, null, null));
    }

    @Test
    public void test_getCrossReference() throws SQLException {
        assertEmptyResultSet(14,
                dbMetaData.getCrossReference(null, null, null, null, null, null));
    }

    @Test
    public void test_getTypeInfo() throws SQLException {
        assertsResultsExactly(asList(
                        asList("TINYINT", Types.TINYINT, 0, null, null, null, 1, true, 3, true, false, false, null, 0, 0, null, null, 10),
                        asList("BIGINT", Types.BIGINT, 0, null, null, null, 1, true, 3, true, false, false, null, 0, 0, null, null, 10),
                        asList("DECIMAL", Types.DECIMAL, 0, null, null, null, 1, true, 3, true, false, false, null, 0, 0, null, null, 10),
                        asList("INT", Types.INTEGER, 0, null, null, null, 1, true, 3, true, false, false, null, 0, 0, null, null, 10),
                        asList("SMALLINT", Types.SMALLINT, 0, null, null, null, 1, true, 3, true, false, false, null, 0, 0, null, null, 10),
                        asList("REAL", Types.REAL, 0, null, null, null, 1, true, 3, true, false, false, null, 0, 0, null, null, 10),
                        asList("DOUBLE", Types.DOUBLE, 0, null, null, null, 1, true, 3, true, false, false, null, 0, 0, null, null, 10),
                        asList("VARCHAR", Types.VARCHAR, 0, null, null, null, 1, true, 3, false, false, false, null, 0, 0, null, null, 10),
                        asList("BOOLEAN", Types.BOOLEAN, 0, null, null, null, 1, true, 3, false, false, false, null, 0, 0, null, null, 10),
                        asList("DATE", Types.DATE, 0, null, null, null, 1, true, 3, false, false, false, null, 0, 0, null, null, 10),
                        asList("TIME", Types.TIME, 0, null, null, null, 1, true, 3, false, false, false, null, 0, 0, null, null, 10),
                        asList("TIMESTAMP", Types.TIMESTAMP, 0, null, null, null, 1, true, 3, false, false, false, null, 0, 0, null, null, 10),
                        asList("JSON", Types.OTHER, 0, null, null, null, 1, true, 3, false, false, false, null, 0, 0, null, null, 10),
                        asList("OBJECT", Types.JAVA_OBJECT, 0, null, null, null, 1, true, 3, false, false, false, null, 0, 0, null, null, 10),
                        asList("TIMESTAMP WITH TIME ZONE", Types.TIMESTAMP_WITH_TIMEZONE, 0, null, null, null, 1, true, 3, false, false, false, null, 0, 0, null, null, 10)),
                dbMetaData.getTypeInfo());

        // check that types are ordered by the DATA_TYPE column
        ResultSet rs = dbMetaData.getTypeInfo();
        Integer lastValue = Integer.MIN_VALUE;
        while (rs.next()) {
            int thisValue = rs.getInt("DATA_TYPE");
            assertThat(thisValue).isGreaterThan(lastValue);
            lastValue = thisValue;
        }
    }

    @Test
    public void test_getIndexInfo() throws SQLException {
        assertEmptyResultSet(13,
                dbMetaData.getIndexInfo(null, null, null, false, false));
    }

    @Test
    public void test_getUDTs() throws SQLException {
        assertEmptyResultSet(7,
                dbMetaData.getUDTs(null, null, null, null));
    }

    @Test
    public void test_getSuperTypes() throws SQLException {
        assertEmptyResultSet(6,
                dbMetaData.getSuperTypes(null, null, null));
    }

    @Test
    public void test_getSuperTables() throws SQLException {
        assertEmptyResultSet(4,
                dbMetaData.getSuperTables(null, null, null));
    }

    @Test
    public void test_getAttributes() throws SQLException {
        assertEmptyResultSet(21,
                dbMetaData.getAttributes(null, null, null, null));
    }

    @Test
    public void test_getClientInfoProperties() throws SQLException {
        assertEmptyResultSet(4,
                dbMetaData.getClientInfoProperties());
    }

    @Test
    public void test_getFunctions() throws SQLException {
        assertEmptyResultSet(6,
                dbMetaData.getFunctions(null, null, null));
    }

    @Test
    public void test_getFunctionColumns() throws SQLException {
        assertEmptyResultSet(17,
                dbMetaData.getFunctionColumns(null, null, null, null));
    }

    @Test
    public void test_getPseudoColumns() throws SQLException {
        assertEmptyResultSet(12,
                dbMetaData.getPseudoColumns(null, null, null, null));
    }

    @Test
    public void test_getSchemas() throws SQLException {
        assertsResultsExactly(singletonList(asList("public", "hazelcast")),
                dbMetaData.getSchemas());
    }

    private void assertsResultsExactly(List<List<Object>> expectedRows, ResultSet resultSet) throws SQLException {
        int columnCount = resultSet.getMetaData().getColumnCount();
        List<List<Object>> actualRows = new ArrayList<>();
        while (resultSet.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 0; i < columnCount; i++) {
                row.add(resultSet.getObject(i + 1));
            }
            actualRows.add(row);
        }
        assertThat(actualRows).containsExactlyElementsOf(expectedRows);
    }

    private void assertEmptyResultSet(int expectedNumColumns, ResultSet resultSet) throws SQLException {
        assertEquals("number of columns", expectedNumColumns, resultSet.getMetaData().getColumnCount());
        assertsResultsExactly(emptyList(), resultSet);
    }
}
