package at.willhaben.jdbcproxy.driver;

import at.willhaben.jdbcproxy.server.avro.DBMetaData;
import at.willhaben.jdbcproxy.server.avro.MetaDataResponse;
import at.willhaben.jdbcproxy.server.avro.TableColumnMetaData;
import at.willhaben.jdbcproxy.server.avro.TableMetaData;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents metadata about the remote database that a proxy-driver is connected to, including the set of
 * tables that it contains.
 * <p>
 * This driver is intended to support basic exploration of the database and execution of queries. Full
 * "discovery" of the database configuration and feature-set is irrelevant for this use-case; methods such
 * as "supportsAlterTable()" are therefore stubbed out (have a fixed return value).
 * </p>
 * <p>
 * Many of the methods appear intended for use by graphical DB tools that allow users to compose queries via
 * drag-and-drop etc. This proxy isn't intended to support this - just simpler tools where the user browses
 * tables and then writes SQL by hand. Therefore many of these methods can return empty data. If you discover
 * a client who cares about some of these methods, then feel free to enhance the data returned from the server
 * to the client (ie extend the AVRO metadata type) and then update the methods here to report the relevant
 * fields to the client/caller.
 * </p>
 * <p>
 * Invocation of stored-procedures is not supported; only plain SQL queries.
 * </p>
 */
@SuppressWarnings("squid:S1192") // sonarqube: some duplicated literals are deliberate, as they have distinct meanings
public class ProxyDatabaseMetaData implements DatabaseMetaData {
    public static final String DRIVER_NAME = "auditproxy";

    public static ProxyDatabaseMetaData of(MetaDataResponse response) {
        return new ProxyDatabaseMetaData(response.getCatalog(), response.getDatabaseMetaData(), response.getTables());
    }

    private final String catalog;
    private final String identifierQuoteString;
    private final List<TableMetaData> tables;

    public ProxyDatabaseMetaData(String catalog, DBMetaData dbMetaData, List<TableMetaData> tables) {
        this.catalog = catalog;
        this.identifierQuoteString = dbMetaData.getIdentifierQuoteString();
        this.tables = tables;
    }

    // ===============================================================

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return "unknown";
    }

    @Override
    public String getUserName() throws SQLException {
        return "unknown";
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return DRIVER_NAME;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "1.0";
    }

    @Override
    public String getDriverName() throws SQLException {
        // The "driver" here really is this proxy - ie the featureset available is limited to what this
        // proxy provides. So return info on this driver, not the remote one used to actually talk to the DB
        return DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "1.0";
    }

    @Override
    public int getDriverMajorVersion() {
        return 1;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return identifierQuoteString;
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "%";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        // for now, limit access to just the default schema of the user configured in the server
        // for the target DB.
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        // not supported; return an empty result-set with the right "shape" but no data
        var metadata = new ProxyResultSetMetaData(
                "procedures",
                List.of("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                        "reserved1", "reserved2", "reserved3",
                        "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.OTHER, Types.OTHER, Types.OTHER,
                        Types.VARCHAR, Types.SMALLINT, Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        // Not supported, so return an empty result-set. The standard result-set format returned by this method has 20
        // columns; as most clients don't actually care (and we don't support introspection of stored-procs here anyway),
        // don't bother to declare correct metadata.
        var metadata = new ProxyResultSetMetaData(
                "procedureColumns",
                List.of("column"),
                List.of(Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        var desiredTypes = (types==null) ? null : Arrays.asList(types);

        // Some client applications get a table-name like "foo_bar" reported, and then invoke this
        // method with table-name of "foo%%_bar", ie the escaped name. Undo that...
        var schemaName = unescape(schemaPattern);
        var tableName = unescape(tableNamePattern);

        // Return a row with 5 columns:
        // 1: catalog
        // 2: schema
        // 3: name
        // 4: type (eg VIEW)
        // 5: remarks
        List<List<?>> tableNames = tables.stream()
                .filter(t -> (catalog == null) || catalog.equals(t.getCatalogName().orElse(null)))
                .filter(t -> (schemaName == null) || schemaName.equals(t.getSchemaName().orElse(null)))
                .filter(t -> (tableName == null) || tableName.equals(t.getName()))
                .filter(t -> desiredTypes == null ? true : desiredTypes.contains(t.getType()))
                .map(t -> listOf(
                        catalog,
                        t.getSchemaName().orElse(null),
                        t.getName(),
                        t.getType(),
                        t.getRemarks().orElse(null)))
                .collect(Collectors.toUnmodifiableList());

        var metadata = new ProxyResultSetMetaData(
                "tables",
                List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE",
                        "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
                        "SELF_REFERENCING_COL_NAME", "REF_GENERATION"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR));
        return ProxyMemResultSet.of(metadata, tableNames);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        var set = tables.stream()
                .map(TableMetaData::getSchemaName)
                .map(Optional::get)
                .collect(Collectors.toUnmodifiableSet());
        var values = new ArrayList<>(set);
        List<List<?>> rows = values.stream().sorted().map(item -> listOf(item, catalog)).collect(Collectors.toUnmodifiableList());

        // return a result-set with a single schema.
        var metadata = new ProxyResultSetMetaData(
                "schemas",
                List.of("TABLE_SCHEM", "TABLE_CATALOG"),
                List.of(Types.VARCHAR, Types.VARCHAR));
        return ProxyMemResultSet.of(metadata, rows);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        // return a result-set with a single catalog.
        var metadata = new ProxyResultSetMetaData("catalogs", List.of("TABLE_CAT"), List.of(Types.VARCHAR));
        List<?> row0 = List.of(catalog);
        List<List<?>> rows = List.of(row0);
        return ProxyMemResultSet.of(metadata, rows);
    }

    /**
     * Return a ResultSet object wrapping a single column of string values where the strings
     * are the kinds of table that are present in the target database.
     * <p>
     * This proxy does not support browsing system-tables, temporary-tables, aliases-etc - just plain
     * tables and views.
     * </p>
     */
    @Override
    public ResultSet getTableTypes() throws SQLException {
        var set = tables.stream().map(TableMetaData::getType).collect(Collectors.toUnmodifiableSet());
        var values = new ArrayList<>(set);
        List<List<?>> valuePerRow = values.stream().sorted().map(List::of).collect(Collectors.toUnmodifiableList());
        var metadata = new ProxyResultSetMetaData("tableTypes", List.of("TABLE_TYPE"), List.of(Types.VARCHAR));
        return ProxyMemResultSet.of(metadata, valuePerRow);
    }

    private String unescape(String pattern) {
        // cheap-and-nasty SQL-unescaping (just enough)
        if (pattern == null) {
            return pattern;
        }

        if (pattern.equals("%")) {
            return null; // wildcard (equivalent to glob-pattern "*")
        }

        return pattern.replace("%%_", "_");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        // Some client applications get a table-name like "foo_bar" reported, and then invoke this
        // method with table-name of "foo%%_bar", ie the escaped name. Undo that...
        var schemaName = unescape(schemaPattern);
        var tableName = unescape(tableNamePattern);
        var columnName = unescape(columnNamePattern);

        var matchingTables = tables.stream()
                .filter(t -> (catalog == null) || catalog.equals(t.getCatalogName().orElse(null)))
                .filter(t -> (schemaName == null) || schemaName.equals(t.getSchemaName().orElse(null)))
                .filter(t -> (tableName == null) || tableName.equals(t.getName()))
                .collect(Collectors.toUnmodifiableList());

        var coldata = matchingTables.stream()
                .flatMap(t -> extractColumns(t, columnName))
                .collect(Collectors.toUnmodifiableList());

        var metadata = new ProxyResultSetMetaData(
                "JDBC_COLUMNS",
                List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                        "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
                        "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
                        "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                        "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA",
                        "SCOPE_TABLE", "SOURCE_DATA_TYPE"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                        Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR,
                        Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                        Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.INTEGER));
        return ProxyMemResultSet.of(metadata, coldata);
    }

    /**
     * Given metadata about a table, map the metadata about its columns into a list of values that can
     * be represented as a ResultSet in the form documented by DatabaseMetaData.getColumns().
     */
    Stream<List<?>> extractColumns(TableMetaData t, String columnName) {
        return t.getColumns().stream()
            .filter(c -> (columnName == null) || columnName.equals(c.getName()))
            .map(c -> listOf(
                catalog, // 1: TABLE_CAT
                t.getSchemaName().orElse(null), // 2: TABLE_SCHEM
                t.getName(), // 3: TABLE_NAME
                c.getName(), // 4: COLUMN_NAME
                c.getType(), // 5: DATA_TYPE
                SqlTypeUtils.toTypeName(c.getType()), // 6: TYPE_NAME
                c.getSize(), // 7: COLUMN_SIZE
                null, // 8: BUFFER_LENGTH
                c.getPrecision(), // 9: DECIMAL_DIGITS
                null, // 10: radix
                c.getNullable(), // 11: NULLABLE
                null, // 12: REMARKS
                null, // 13: COLUMN_DEF
                null, // 14; SQL_DATA_TYPE
                null, // 15: SQL_DATETIME_SUB
                c.getCharOctetLength(), // 16: CHAR_OCTET_LENGTH (max bytes in this column)
                c.getOrdinal(), // 17: ORDINAL_POSITION
                getISONullable(c), // 18: IS_NULLABLE (ISO format) ("YES"/"NO"/"")
                null, // 19: SCOPE_CATALOG
                null, // 20: SCOPE_SCHEMA
                null, // 21: SCOPE_TABLE
                null // 22: SOURCE_DATA_TYPE
        ));
    }

    private String getISONullable(TableColumnMetaData md) {
        var state = md.getNullable();
        switch(state) {
            case DatabaseMetaData.columnNoNulls: return "NO";
            case DatabaseMetaData.columnNullable: return "YES";
            default: return ""; // unknown
        }
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        // not supported; return an empty result-set with the right "shape" but no data
        var metadata = new ProxyResultSetMetaData(
                "columnPrivileges",
                List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                        "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        // not supported; return an empty result-set with the right "shape" but no data
        var metadata = new ProxyResultSetMetaData(
                "tablePrivileges",
                List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR",
                        "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        // not supported; return an empty result-set with the right "shape" but no data
        var metadata = new ProxyResultSetMetaData(
                "bestRowIdentifier",
                List.of("SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                        "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"),
                List.of(Types.SMALLINT, Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
                        Types.INTEGER, Types.INTEGER, Types.SMALLINT, Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        // not supported; return an empty result-set with the right "shape" but no data
        var metadata = new ProxyResultSetMetaData(
                "versionColumns",
                List.of("SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                        "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"),
                List.of(Types.SMALLINT, Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
                        Types.INTEGER, Types.INTEGER, Types.SMALLINT, Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        // not supported; return an empty result-set with the right "shape" but no data
        var metadata = new ProxyResultSetMetaData(
                "primaryKeys",
                List.of("TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        // not supported; Return an empty result-set. When the resultset is empty, most/all clients don't
        // actually care about the metadata (resultset schema) so just provide a mock one.
        var metadata = new ProxyResultSetMetaData("importedKeys", List.of("key"), List.of(Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        // not supported; Return an empty result-set. When the resultset is empty, most/all clients don't
        // actually care about the metadata (resultset schema) so just provide a mock one.
        var metadata = new ProxyResultSetMetaData("exportedKeys", List.of("key"), List.of(Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        // not supported; Return an empty result-set. When the resultset is empty, most/all clients don't
        // actually care about the metadata (resultset schema) so just provide a mock one.
        var metadata = new ProxyResultSetMetaData("crossReference", List.of("key"), List.of(Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        // not supported; Return an empty result-set. When the resultset is empty, most/all clients don't
        // actually care about the metadata (resultset schema) so just provide a mock one.
        var metadata = new ProxyResultSetMetaData("types", List.of("type"), List.of(Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        // not supported; Return an empty result-set. When the resultset is empty, most/all clients don't
        // actually care about the metadata (resultset schema) so just provide a mock one.
        var metadata = new ProxyResultSetMetaData("indexInfo", List.of("index"), List.of(Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        // This should be enough for simple DB GUIs to present results; even if the real underlying database
        // supports more we don't need it so don't offer it.
        return (type == ResultSet.TYPE_FORWARD_ONLY);
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        // not supported; Return an empty result-set. When the resultset is empty, most/all clients don't
        // actually care about the metadata (resultset schema) so just provide a mock one.
        var metadata = new ProxyResultSetMetaData("UDTs", List.of("UDT"), List.of(Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw new UnsupportedOperationException("DatabaseMetaData.getConnection");
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        // not supported; Return an empty result-set. When the resultset is empty, most/all clients don't
        // actually care about the metadata (resultset schema) so just provide a mock one.
        var metadata = new ProxyResultSetMetaData("superTypes", List.of("type"), List.of(Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        // not supported; Return an empty result-set. When the resultset is empty, most/all clients don't
        // actually care about the metadata (resultset schema) so just provide a mock one.
        var metadata = new ProxyResultSetMetaData("superTables", List.of("table"), List.of(Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        // not supported; Return an empty result-set. When the resultset is empty, most/all clients don't
        // actually care about the metadata (resultset schema) so just provide a mock one.
        var metadata = new ProxyResultSetMetaData("attributes", List.of("attribute"), List.of(Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return (holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    /**
     * Emulate JDBC-4.3.
     * <p>
     * It doesn't really matter what the target database supports; this proxy supports only very simple
     * operations (browsing tables and views and executing queries) so any sane backend will be sufficient.
     * </p>
     */
    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 3;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return DatabaseMetaData.sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        // get unique schemas
        var schemas = tables.stream()
                .map(t ->t.getSchemaName())
                .map(s -> s.get())
                .collect(Collectors.toUnmodifiableSet());
        List<List<?>> schemaPerRow = schemas.stream().sorted()
                .map(i -> List.of(i, catalog))
                .collect(Collectors.toUnmodifiableList());

        var metadata = new ProxyResultSetMetaData(
                "schemas",
                List.of("TABLE_SCHEM", "TABLE_CATALOG"),
                List.of(Types.VARCHAR, Types.VARCHAR));
        return ProxyMemResultSet.of(metadata, schemaPerRow);
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        // not supported; Return an empty result-set. When the resultset is empty, most/all clients don't
        // actually care about the metadata (resultset schema) so just provide a mock one.
        var metadata = new ProxyResultSetMetaData("clientInfoProperties", List.of("property"), List.of(Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        // not supported; Return an empty result-set. When the resultset is empty, most/all clients don't
        // actually care about the metadata (resultset schema) so just provide a mock one.
        var metadata = new ProxyResultSetMetaData("functions", List.of("function"), List.of(Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        // not supported; Return an empty result-set. When the resultset is empty, most/all clients don't
        // actually care about the metadata (resultset schema) so just provide a mock one.
        var metadata = new ProxyResultSetMetaData("functionColumns", List.of("column"), List.of(Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        // not supported; Return an empty result-set. When the resultset is empty, most/all clients don't
        // actually care about the metadata (resultset schema) so just provide a mock one.
        var metadata = new ProxyResultSetMetaData("pseudoColumns", List.of("column"), List.of(Types.VARCHAR));
        return ProxyMemResultSet.of(metadata);
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("DatabaseMetaData.unwrap");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    // Like List.of() except that it allows null members..
    private static <T> List<T> listOf(T... args) {
        var l = new ArrayList<T>(Arrays.asList(args));
        return Collections.unmodifiableList(l);
    }
}