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

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;

@SuppressWarnings("checkstyle:TrailingComment")
public class JdbcDataBaseMetadata implements DatabaseMetaData {
    private static final int JDBC_VERSION_MAJOR = 5;
    private static final int JDBC_VERSION_MINOR = 5;
    private static final int DEFAULT_NUMBER_RADIX = 10;

    private final JdbcConnection connection;

    public JdbcDataBaseMetadata(JdbcConnection connection) {
        this.connection = connection;
    }

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public String getURL() {
        return connection.getJdbcUrl().getRawUrl();
    }

    @Override
    @SuppressFBWarnings("NP_NONNULL_RETURN_VIOLATION")
    public String getUserName() {
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    @Override
    public String getDatabaseProductName() {
        return "Hazelcast";
    }

    @Override
    public String getDatabaseProductVersion() {
        return this.getMasterVersion().toString();
    }

    @Override
    public String getDriverName() {
        return "Hazelcast JDBC";
    }

    @Override
    public String getDriverVersion() {
        return Driver.VER_MAJOR + "." + Driver.VER_MINOR;
    }

    @Override
    public int getDriverMajorVersion() {
        return Driver.VER_MAJOR;
    }

    @Override
    public int getDriverMinorVersion() {
        return Driver.VER_MINOR;
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() {
        return "\"";
    }

    @Override
    public String getSQLKeywords() {
        return "";
    }

    @Override
    public String getNumericFunctions() {
        return "ABS,CEIL,DEGREES,EXP,FLOOR,LN,LOG10,LOG10,RAND,ROUND,SIGN,TRUNCATE,ACOS,ASIN,ATAN,COS,COT,SIN,TAN";
    }

    @Override
    public String getStringFunctions() {
        return "ASCII,BTRIM,INITCAP,LENGTH,LIKE,ESCAPE,LOWER,LTRIM,RTRIM,SUBSTRING,TRIM,UPPER";
    }

    @Override
    public String getSystemFunctions() {
        return "";
    }

    @Override
    public String getTimeDateFunctions() {
        return "";
    }

    @Override
    public String getSearchStringEscape() {
        return "";
    }

    @Override
    public String getExtraNameCharacters() {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return true;
    }

    @Override
    public boolean supportsConvert() {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return true;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return true;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return false;
    }

    @Override
    public String getSchemaTerm() {
        return "schema";
    }

    @Override
    public String getProcedureTerm() {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() {
        return true;
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return true;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    @Override
    public boolean supportsUnion() {
        return false;
    }

    @Override
    public boolean supportsUnionAll() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 0;
    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public int getMaxIndexLength() {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return 0;
    }

    @Override
    public int getMaxRowSize() {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return false;
    }

    @Override
    public int getMaxStatementLength() {
        return 0;
    }

    @Override
    public int getMaxStatements() {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return level == Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    @Override
    public ResultSet getProcedures(
            String catalog,
            String schemaPattern,
            String procedureNamePattern
    ) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("PROCEDURE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("PROCEDURE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("PROCEDURE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("EXPR$0", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("EXPR$1", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("EXPR$2", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("REMARKS", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("PROCEDURE_TYPE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("SPECIFIC_NAME", SqlColumnType.VARCHAR, false)
        )));
    }

    @Override
    public ResultSet getProcedureColumns(
            String catalog,
            String schemaPattern,
            String procedureNamePattern,
            String columnNamePattern
    ) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("PROCEDURE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("PROCEDURE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("PROCEDURE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("COLUMN_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("COLUMN_TYPE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("DATA_TYPE", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("TYPE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("PRECISION", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("LENGTH", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("SCALE", SqlColumnType.SMALLINT, true),
                new SqlColumnMetadata("RADIX", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("NULLABLE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("REMARKS", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("COLUMN_DEF", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("SQL_DATA_TYPE", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("SQL_DATETIME_SUB", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("CHAR_OCTET_LENGTH", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("ORDINAL_POSITION", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("IS_NULLABLE", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("SPECIFIC_NAME", SqlColumnType.VARCHAR, false)
        )));
    }

    @Override
    @SuppressFBWarnings({"OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE", "ODR_OPEN_DATABASE_RESOURCE"})
    public ResultSet getTables(String catalog, String schema, String tableName, String[] types) throws SQLException {
        final List<Object> params = new ArrayList<>();
        final StringBuilder sqlBuilder = new StringBuilder("SELECT "
                + "table_catalog TABLE_CAT,"
                + "table_schema TABLE_SCHEM,"
                + "table_name TABLE_NAME,"
                + "CASE table_type WHEN 'BASE TABLE' THEN 'MAPPING' ELSE table_type END TABLE_TYPE, "
                + "CAST(null as VARCHAR) REMARKS,"
                + "CAST(null as VARCHAR) TYPE_CAT,"
                + "CAST(null as VARCHAR) TYPE_SCHEM,"
                + "CAST(null as VARCHAR) TYPE_NAME,"
                + "CAST(null as VARCHAR) SELF_REFERENCING_COL_NAME,"
                + "CAST(null as VARCHAR) REF_GENERATION "
                + "FROM information_schema.tables "
                + "WHERE 1=1");

        if (catalog != null) {
            sqlBuilder.append(" AND table_catalog LIKE ?");
            params.add(catalog);
        }

        if (schema != null) {
            sqlBuilder.append(" AND table_schema LIKE ?");
            params.add(schema);
        }

        if (tableName != null) {
            sqlBuilder.append(" AND table_name LIKE ?");
            params.add(tableName);
        }

        if (types != null && types.length > 0) {
            sqlBuilder.append(" AND table_type IN ("
                    + stream(types).map(s -> "?").collect(joining(","))
                    + ")"
            );
            stream(types)
                    .map(type -> type.equals("MAPPING") ? "BASE TABLE" : type)
                    .forEach(params::add);
        }

        sqlBuilder.append(" ORDER BY TABLE_TYPE, table_catalog, table_schema, table_name");

        PreparedStatement statement = connection.prepareStatement(sqlBuilder.toString());
        for (int i = 0; i < params.size(); i++) {
            statement.setObject(i + 1, params.get(i));
        }
        return statement.executeQuery();
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        final SqlRowMetadata metadata = new SqlRowMetadata(asList(
                new SqlColumnMetadata("TABLE_SCHEM", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("TABLE_CATALOG", SqlColumnType.VARCHAR, true)
        ));

        final List<SqlRow> rows = singletonList(makeSqlRow(new Object[]{"public", "hazelcast"}, metadata));

        return new JdbcResultSet(new FixedRowsSqlResult(metadata, rows), new JdbcStatement(null, connection));
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return getSchemas();
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        final SqlRowMetadata metadata = new SqlRowMetadata(singletonList(new SqlColumnMetadata(
                "TABLE_CAT",
                SqlColumnType.VARCHAR,
                false
        )));

        final List<SqlRow> rows = singletonList(makeSqlRow(new Object[]{"hazelcast"}, metadata));

        return new JdbcResultSet(new FixedRowsSqlResult(metadata, rows), new JdbcStatement(null, connection));
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        final SqlRowMetadata metadata = new SqlRowMetadata(singletonList(new SqlColumnMetadata(
                "TABLE_TYPE",
                SqlColumnType.VARCHAR,
                false
        )));

        final List<SqlRow> rows = asList(
                makeSqlRow(new Object[]{"MAPPING"}, metadata),
                makeSqlRow(new Object[]{"VIEW"}, metadata)
        );

        return new JdbcResultSet(new FixedRowsSqlResult(metadata, rows), new JdbcStatement(null, connection));
    }

    @Override
    @SuppressWarnings({
            "checkstyle:CyclomaticComplexity",
            "checkstyle:MethodLength",
            "checkstyle:NPathComplexity"
    })
    public ResultSet getColumns(
            String catalog,
            String schema,
            String tableName,
            String columnName
    ) throws SQLException {
        final List<String> conditions = new ArrayList<>();
        final List<Object> params = new ArrayList<>();
        final StringBuilder sqlBuilder = new StringBuilder("SELECT "
                + "table_catalog,"
                + "table_schema,"
                + "table_name,"
                + "column_name, "
                + "data_type,"
                + "is_nullable,"
                + "ordinal_position "
                + "FROM information_schema.columns ");

        if (catalog != null) {
            conditions.add("table_catalog LIKE ?");
            params.add(catalog);
        }

        if (schema != null) {
            conditions.add("table_schema LIKE ?");
            params.add(schema);
        }

        if (tableName != null) {
            conditions.add("table_name LIKE ?");
            params.add(tableName);
        }

        if (columnName != null) {
            conditions.add("column_name LIKE ?");
            params.add(columnName);
        }

        if (!conditions.isEmpty()) {
            sqlBuilder.append(" WHERE ");
            sqlBuilder.append(String.join(" AND ", conditions));
        }

        sqlBuilder.append(" ORDER BY table_catalog, table_schema, table_name, ordinal_position ASC");

        final SqlRowMetadata metadata = new SqlRowMetadata(asList(
                new SqlColumnMetadata("TABLE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TABLE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TABLE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("COLUMN_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("DATA_TYPE", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("TYPE_NAME", SqlColumnType.VARCHAR, false),

                new SqlColumnMetadata("COLUMN_SIZE", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("BUFFER_LENGTH", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("DECIMAL_DIGITS", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("NUM_PREC_RADIX", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("NULLABLE", SqlColumnType.INTEGER, false),

                new SqlColumnMetadata("REMARKS", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("COLUMN_DEF", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("SQL_DATA_TYPE", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("SQL_DATETIME_SUB", SqlColumnType.INTEGER, true),

                new SqlColumnMetadata("CHAR_OCTET_LENGTH", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("ORDINAL_POSITION", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("IS_NULLABLE", SqlColumnType.VARCHAR, false),

                new SqlColumnMetadata("SCOPE_CATALOG", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("SCOPE_SCHEMA", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("SCOPE_TABLE", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("SOURCE_DATA_TYPE", SqlColumnType.SMALLINT, true),
                new SqlColumnMetadata("IS_AUTOINCREMENT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("IS_GENERATEDCOLUMN", SqlColumnType.VARCHAR, true)
        ));

        final List<SqlRow> rows = new ArrayList<>();
        try (PreparedStatement statement = this.connection.prepareStatement(sqlBuilder.toString())) {
            for (int i = 0; i < params.size(); i++) {
                statement.setObject(i + 1, params.get(i));
            }
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    final SqlColumnType sqlColumnType = TypeUtil.getTypeByQDTName(rs.getString("data_type"));
                    final boolean isNullable = rs.getBoolean("is_nullable");
                    final TypeUtil.SqlTypeInfo typeInfo = TypeUtil.getTypeInfo(sqlColumnType);

                    rows.add(makeSqlRow(new Object[]{
                            rs.getString("table_catalog"),
                            rs.getString("table_schema"),
                            rs.getString("table_name"),
                            rs.getString("column_name"),
                            TypeUtil.getJdbcType(sqlColumnType), // DATA_TYPE
                            // Source column is QueryDataTypeFamily.name()
                            rs.getString("data_type").replaceAll("_", " "), // TYPE_NAME

                            typeInfo.getPrecision(), // COLUMN_SIZE
                            null, // BUFFER_LENGTH
                            typeInfo.getScale() == 0 ? null : typeInfo.getScale(), // DECIMAL_DIGITS
                            DEFAULT_NUMBER_RADIX, // NUM_PREC_RADIX
                            isNullable ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls, // NULLABLE

                            null, // REMARKS
                            null, // COLUMN_DEF
                            null, // SQL_DATA_TYPE
                            null, // SQL_DATETIME_SUB

                            sqlColumnType.equals(SqlColumnType.VARCHAR) ? typeInfo.getPrecision() : null, // CHAR_OCTET_LENGTH
                            rs.getInt("ordinal_position"),
                            isNullable ? "YES" : "NO", // IS_NULLABLE

                            null, // SCOPE_CATALOG
                            null, // SCOPE_SCHEMA
                            null, // SCOPE_TABLE
                            null, // SOURCE_DATA_TYPE
                            "", // IS_AUTOINCREMENT
                            "" // IS_GENERATEDCOLUMN
                    }, metadata));
                }
            }
        }

        return new JdbcResultSet(new FixedRowsSqlResult(metadata, rows), new JdbcStatement(null, connection));
    }

    @Override
    public ResultSet getColumnPrivileges(
            String catalog,
            String schema,
            String table,
            String columnNamePattern
    ) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("TABLE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TABLE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TABLE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("COLUMN_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("GRANTOR", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("GRANTEE", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("PRIVILEGE", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("IS_GRANTABLE", SqlColumnType.VARCHAR, true)
        )));
    }

    @Override
    public ResultSet getTablePrivileges(
            String catalog,
            String schemaPattern,
            String tableNamePattern
    ) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("TABLE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TABLE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TABLE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("GRANTOR", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("GRANTEE", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("PRIVILEGE", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("IS_GRANTABLE", SqlColumnType.VARCHAR, true)
        )));
    }

    @Override
    public ResultSet getBestRowIdentifier(
            String catalog,
            String schema,
            String table,
            int scope,
            boolean nullable
    ) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("SCOPE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("COLUMN_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("DATA_TYPE", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("TYPE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("COLUMN_SIZE", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("BUFFER_LENGTH", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("DECIMAL_DIGITS", SqlColumnType.SMALLINT, true),
                new SqlColumnMetadata("PSEUDO_COLUMN", SqlColumnType.SMALLINT, false)
        )));
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("SCOPE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("COLUMN_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("DATA_TYPE", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("TYPE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("COLUMN_SIZE", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("BUFFER_LENGTH", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("DECIMAL_DIGITS", SqlColumnType.SMALLINT, true),
                new SqlColumnMetadata("PSEUDO_COLUMN", SqlColumnType.SMALLINT, false)
        )));
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("TABLE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TABLE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TABLE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("COLUMN_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("KEY_SEQ", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("PK_NAME", SqlColumnType.VARCHAR, true)
        )));
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("PKTABLE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("PKTABLE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("PKTABLE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("PKCOLUMN_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("FKTABLE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("FKTABLE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("FKTABLE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("FKCOLUMN_NAME", SqlColumnType.VARCHAR, false),

                new SqlColumnMetadata("KEY_SEQ", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("UPDATE_RULE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("DELETE_RULE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("FK_NAME", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("PK_NAME", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("DEFERRABILITY", SqlColumnType.SMALLINT, false)
        )));
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("PKTABLE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("PKTABLE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("PKTABLE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("PKCOLUMN_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("FKTABLE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("FKTABLE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("FKTABLE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("FKCOLUMN_NAME", SqlColumnType.VARCHAR, false),

                new SqlColumnMetadata("KEY_SEQ", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("UPDATE_RULE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("DELETE_RULE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("FK_NAME", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("PK_NAME", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("DEFERRABILITY", SqlColumnType.SMALLINT, false)
        )));
    }

    @Override
    public ResultSet getCrossReference(
            String parentCatalog,
            String parentSchema,
            String parentTable,
            String foreignCatalog,
            String foreignSchema,
            String foreignTable
    ) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("PKTABLE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("PKTABLE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("PKTABLE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("PKCOLUMN_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("FKTABLE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("FKTABLE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("FKTABLE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("FKCOLUMN_NAME", SqlColumnType.VARCHAR, false),

                new SqlColumnMetadata("KEY_SEQ", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("UPDATE_RULE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("DELETE_RULE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("FK_NAME", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("PK_NAME", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("DEFERRABILITY", SqlColumnType.SMALLINT, false)
        )));
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        final SqlRowMetadata metadata = new SqlRowMetadata(asList(
                new SqlColumnMetadata("TYPE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("DATA_TYPE", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("PRECISION", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("LITERAL_PREFIX", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("LITERAL_SUFFIX", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("CREATE_PARAMS", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("NULLABLE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("CASE_SENSITIVE", SqlColumnType.BOOLEAN, false),
                new SqlColumnMetadata("SEARCHABLE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("UNSIGNED_ATTRIBUTE", SqlColumnType.BOOLEAN, false),
                new SqlColumnMetadata("FIXED_PREC_SCALE", SqlColumnType.BOOLEAN, false),
                new SqlColumnMetadata("AUTO_INCREMENT", SqlColumnType.BOOLEAN, false),
                new SqlColumnMetadata("LOCAL_TYPE_NAME", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("MINIMUM_SCALE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("MAXIMUM_SCALE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("SQL_DATA_TYPE", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("SQL_DATETIME_SUB", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("NUM_PREC_RADIX", SqlColumnType.INTEGER, false)
        ));

        final List<SqlRow> rows = asList(
                typeInfoRow(SqlColumnType.VARCHAR, metadata),
                typeInfoRow(SqlColumnType.BOOLEAN, metadata),
                typeInfoRow(SqlColumnType.BIGINT, metadata),
                typeInfoRow(SqlColumnType.TINYINT, metadata),
                typeInfoRow(SqlColumnType.SMALLINT, metadata),
                typeInfoRow(SqlColumnType.INTEGER, metadata),
                typeInfoRow(SqlColumnType.DECIMAL, metadata),
                typeInfoRow(SqlColumnType.REAL, metadata),
                typeInfoRow(SqlColumnType.DOUBLE, metadata),
                typeInfoRow(SqlColumnType.TIME, metadata),
                typeInfoRow(SqlColumnType.DATE, metadata),
                typeInfoRow(SqlColumnType.TIMESTAMP, metadata),
                typeInfoRow(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, metadata),
                typeInfoRow(SqlColumnType.OBJECT, metadata),
                typeInfoRow(SqlColumnType.JSON, metadata)
        );

        rows.sort(Comparator.comparing(sqlRow -> sqlRow.getObject(1)));

        return new JdbcResultSet(new FixedRowsSqlResult(metadata, rows), new JdbcStatement(null, connection));
    }

    private SqlRow typeInfoRow(SqlColumnType columnType, SqlRowMetadata metadata) {
        final TypeUtil.SqlTypeInfo typeInfo = TypeUtil.getTypeInfo(columnType);

        final Object[] values = new Object[]{
                TypeUtil.getName(columnType), // TYPE_NAME
                TypeUtil.getJdbcType(columnType), // DATA_TYPE
                0, // PRECISION - it's not defined what it should be. Other vendors also return 0 here.
                null, // LITERAL_PREFIX
                null, // LITERAL_SUFFIX
                null, // CREATE_PARAMS
                DatabaseMetaData.typeNullable, // NULLABLE
                true, // CASE_SENSITIVE
                DatabaseMetaData.typeSearchable, // SEARCHABLE
                typeInfo.isSigned(), // UNSIGNED_ATTRIBUTE
                false, // FIXED_PREC_SCALE
                false, // AUTO_INCREMENT
                null, // LOCAL_TYPE_NAME
                0, // MINIMUM_SCALE
                0, // MAXIMUM_SCALE
                null, // SQL_DATA_TYPE
                null, // SQL_DATETIME_SUB
                DEFAULT_NUMBER_RADIX // NUM_PREC_RADIX
        };
        return makeSqlRow(values, metadata);
    }

    @Override
    public ResultSet getIndexInfo(
            String catalog,
            String schema,
            String table,
            boolean unique,
            boolean approximate
    ) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("TABLE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TABLE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TABLE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("NON_UNIQUE", SqlColumnType.BOOLEAN, false),
                new SqlColumnMetadata("INDEX_QUALIFIER", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("INDEX_NAME", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TYPE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("ORDINAL_POSITION", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("COLUMN_NAME", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("ASC_OR_DESC", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("CARDINALITY", SqlColumnType.BIGINT, false),
                new SqlColumnMetadata("PAGES", SqlColumnType.BIGINT, false),
                new SqlColumnMetadata("FILTER_CONDITION", SqlColumnType.VARCHAR, true)
        )));
    }

    @Override
    public boolean supportsResultSetType(int type) {
        return connection.supportsResultSetType(type);
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return connection.supportsResultSetConcurrency(concurrency);
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return false;
    }

    @Override
    public ResultSet getUDTs(
            String catalog,
            String schemaPattern,
            String typeNamePattern,
            int[] types
    ) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("TYPE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TYPE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TYPE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("CLASS_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("DATA_TYPE", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("REMARKS", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("BASE_TYPE", SqlColumnType.SMALLINT, true)
        )));
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("TYPE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TYPE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TYPE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("SUPERTYPE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("SUPERTYPE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("SUPERTYPE_NAME", SqlColumnType.VARCHAR, false)
        )));
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("TABLE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TABLE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TABLE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("SUPERTABLE_NAME", SqlColumnType.VARCHAR, false)
        )));
    }

    @Override
    public ResultSet getAttributes(
            String catalog,
            String schemaPattern,
            String typeNamePattern,
            String attributeNamePattern
    ) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("TYPE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TYPE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TYPE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("ATTR_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("DATA_TYPE", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("ATTR_TYPE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("ATTR_SIZE", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("DECIMAL_DIGITS", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("NUM_PREC_RADIX", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("NULLABLE", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("REMARKS", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("ATTR_DEF", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("SQL_DATA_TYPE", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("SQL_DATETIME_SUB", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("CHAR_OCTET_LENGTH", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("ORDINAL_POSITION", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("IS_NULLABLE", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("SCOPE_CATALOG", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("SCOPE_SCHEMA", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("SCOPE_TABLE", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("SOURCE_DATA_TYPE", SqlColumnType.SMALLINT, true)
        )));
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return connection.supportsHoldability(holdability);
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() {
        return this.getMasterVersion().getMajor();
    }

    @Override
    public int getDatabaseMinorVersion() {
        return this.getMasterVersion().getMinor();
    }

    @Override
    public int getJDBCMajorVersion() {
        return JDBC_VERSION_MAJOR;
    }

    @Override
    public int getJDBCMinorVersion() {
        return JDBC_VERSION_MINOR;
    }

    @Override
    public int getSQLStateType() {
        return DatabaseMetaData.sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("LEN", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("DEFAULT_VALUE", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("DESCRIPTION", SqlColumnType.VARCHAR, false)
        )));
    }

    @Override
    public ResultSet getFunctions(
            String catalog,
            String schemaPattern,
            String functionNamePattern
    ) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("FUNCTION_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("FUNCTION_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("FUNCTION_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("REMARKS", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("FUNCTION_TYPE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("SPECIFIC_NAME", SqlColumnType.VARCHAR, false)
        )));
    }

    @Override
    public ResultSet getFunctionColumns(
            String catalog,
            String schemaPattern,
            String functionNamePattern,
            String columnNamePattern
    ) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("FUNCTION_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("FUNCTION_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("FUNCTION_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("COLUMN_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("COLUMN_TYPE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("DATA_TYPE", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("TYPE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("PRECISION", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("LENGTH", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("SCALE", SqlColumnType.SMALLINT, true),
                new SqlColumnMetadata("RADIX", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("NULLABLE", SqlColumnType.SMALLINT, false),
                new SqlColumnMetadata("REMARKS", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("CHAR_OCTET_LENGTH", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("ORDINAL_POSITION", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("IS_NULLABLE", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("SPECIFIC_NAME", SqlColumnType.VARCHAR, false)
        )));
    }

    @Override
    public ResultSet getPseudoColumns(
            String catalog,
            String schemaPattern,
            String tableNamePattern,
            String columnNamePattern
    ) throws SQLException {
        return emptyResultSet(new SqlRowMetadata(asList(
                new SqlColumnMetadata("TABLE_CAT", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TABLE_SCHEM", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("TABLE_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("COLUMN_NAME", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("DATA_TYPE", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("COLUMN_SIZE", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("DECIMAL_DIGITS", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("NUM_PREC_RADIX", SqlColumnType.INTEGER, true),
                new SqlColumnMetadata("COLUMN_USAGE", SqlColumnType.VARCHAR, false),
                new SqlColumnMetadata("REMARKS", SqlColumnType.VARCHAR, true),
                new SqlColumnMetadata("CHAR_OCTET_LENGTH", SqlColumnType.INTEGER, false),
                new SqlColumnMetadata("IS_NULLABLE", SqlColumnType.VARCHAR, false)
        )));
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return JdbcUtils.isWrapperFor(this, iface);
    }

    private SqlRow makeSqlRow(Object[] values, SqlRowMetadata sqlRowMetadata) {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        JetSqlRow jetSqlRow = new JetSqlRow(serializationService, values);
        return new SqlRowImpl(sqlRowMetadata, jetSqlRow);
    }

    // See https://github.com/hazelcast/hazelcast/issues/21301
    private Version getMasterVersion() {
        // connection.getClientInstance().getCluster().getClusterVersion();
        MemberVersion memberVersion = connection.getClientInstance().getCluster()
                .getMembers().iterator().next().getVersion();
        return Version.of(memberVersion.getMajor(), memberVersion.getMinor());
    }

    private ResultSet emptyResultSet(final SqlRowMetadata metadata) throws SQLException {
        return new JdbcResultSet(
                new FixedRowsSqlResult(metadata, emptyList()),
                new JdbcStatement(null, connection)
        );
    }
}
