package at.willhaben.jdbcproxy.server;

import at.willhaben.jdbcproxy.server.avro.*;
import at.willhaben.jdbcproxy.server.dao.AuditLogger;
import at.willhaben.jdbcproxy.server.model.Login;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.sql.DatabaseMetaData;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles a sequence of requests (commands) from a connected client (user).
 */
class RequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private final Login login;
    private final List<String> schemas;
    private final int maxRows; // truncate result-sets at this number of rows

    private final Watchdog watchdog;
    private final DataSource dataSource;
    private final AuditLogger auditLogger;

    RequestHandler(
            Login login,
            List<String> schemas,
            int maxRows,
            DataSource dataSource,
            Watchdog watchdog,
            AuditLogger auditLogger) {
        this.login = login;
        this.schemas = schemas;
        this.maxRows = maxRows;
        this.dataSource = dataSource;
        this.watchdog = watchdog;
        this.auditLogger = auditLogger;
    }

    /**
     * Given a request, produce a response.
     */
    SpecificRecord handleRequest(Object request) throws SQLException {
        if (request instanceof MetaDataRequest) {
            return getMetaData();
        }

        if (request instanceof SelectRequest) {
            return executeRequest((SelectRequest) request);
        }

        if (request instanceof PingRequest) {
            return PingResponse.newBuilder().setOk(true).build();
        }

        return null;
    }

    SpecificRecord getMetaData() throws SQLException {
        try (var conn = dataSource.getConnection()) {
            var catalog = conn.getCatalog(); // default catalog for the current connection

            var metaData = conn.getMetaData();
            var response = MetaDataResponse.newBuilder();
            response.setCatalog(catalog);

            // Copy the subset of fields of object metaData which the remote client might care about.
            // There are far too many fields to send them all - unless we go to some hack like serializing
            // metaData to bytes or JSON. Fortunately, most clients need very few of these fields...
            var dbMetaData = DBMetaData.newBuilder();
            dbMetaData.setIdentifierQuoteString(metaData.getIdentifierQuoteString());
            response.setDatabaseMetaData(dbMetaData.build());

            try (var allTables = metaData.getTables(catalog, null, null, null)) {
                var tables = toTableMetaData(allTables, catalog);
                populateColumnMetaData(tables, metaData);
                response.setTables(tables);
            }
            return response.build();
        }
    }

    /**
     * Work around strange Postgres behaviour.
     * <p>
     * When TableMetaDta.getTables("somecatalog", ....) is invoked, then in the returned result-set:
     * + sybase populates column 1 with "somecatalog"
     * + postgres sets column 1 to null
     * </p>
     * <p>
     * To ensure the client gets sane catalog info for each table, use the catalog through which
     * the tables were fetched as a default.
     * </p>
     */
    private String getTableCatalog(String in, String defaultValue) {
        if (in != null) {
            return in;
        }
        return defaultValue;
    }

    /**
     * Get the list of all tables - but with empty column info.
     * <p>
     * Column data is not fetched here because that requires a new query to the database, ie the structure of the
     * AVRO response is quite different from how JDBC returns the data.
     * </p>
     */
    List<TableMetaData> toTableMetaData(ResultSet rs, String catalog) throws SQLException {
        var tables = new ArrayList<TableMetaData>();
        while (rs.next()) {
            var tableCatalog = getTableCatalog(rs.getString(1), catalog);
            var tableSchema = rs.getString(2);
            var tableName = rs.getString(3);

            var tableType = rs.getString(4);
            if (tableType == null) {
                tableType = "Unknown"; // alternative would be to change AVRO schema so TableMetaData.type is nullable
            }

            if ((this.schemas != null) && !this.schemas.contains(tableSchema)) {
                continue;
            }

            var entity = TableMetaData.newBuilder()
                    .setCatalogName(tableCatalog)
                    .setSchemaName(tableSchema)
                    .setName(tableName)
                    .setType(tableType)
                    .setRemarks(rs.getString(5))
                    .setTypeCatalog(null)
                    .setTypeSchema(null)
                    .setTypeName(null)
                    .setColumns(new ArrayList<>())
                    .build();
            tables.add(entity);
        }
        return tables;
    }

    /**
     * Iterate over all tables, updating attribute "colummns".
     */
    void populateColumnMetaData(List<TableMetaData> tables, DatabaseMetaData metaData) throws SQLException {
        for (var t : tables) {
            try (var rs = metaData.getColumns(
                    t.getCatalogName().orElse(null),
                    t.getSchemaName().orElse(null),
                    t.getName(),
                    null)) {
                while (rs.next()) {
                    // See documentation for method DatabaseMetaData.getColumns
                    var ordinal = rs.getInt(17);
                    var colName = rs.getString(4);
                    var sqlType = rs.getInt(5);
                    var colSize = rs.getInt(7);
                    var nDigits = rs.getInt(9);
                    var nullable = rs.getInt(11);
                    var charOctetLength = rs.getInt(16);

                    var column = TableColumnMetaData.newBuilder()
                            .setOrdinal(ordinal)
                            .setName(colName)
                            .setType(sqlType)
                            .setSize(colSize)
                            .setPrecision(nDigits)
                            .setNullable(nullable)
                            .setCharOctetLength(charOctetLength)
                            .build();
                    t.getColumns().add(column);
                }
            }
        }
    }

    SpecificRecord executeRequest(SelectRequest request) {
        try {
            auditLogger.recordOperation(login, request.getQuery());
        } catch(IOException e) {
            var cause = e.getMessage();
            if (cause == null) {
                cause = "unknown";
            }
            String msg = String.format("Unable to create audit trail for select. Reason: %s", cause);
            return ErrorResponse.newBuilder().setMessage(msg).build();
        }

        try (var conn = dataSource.getConnection();
            var stmt = conn.prepareStatement(request.getQuery())) {
                stmt.setMaxRows(maxRows);
                var rs = watchdog.executeQuery(stmt, login.getSession().getUsername());
                var resultMetaData = createResultMetaData(rs.getMetaData());
                var rowData = createRowData(resultMetaData, rs);
                return SelectResponse.newBuilder()
                        .setMetadata(resultMetaData)
                        .setRows(rowData)
                        .build();
        } catch(SQLException e) {
            var msg = e.getMessage();
            if (msg == null) {
                msg = "Unknown error while executing statement";
            }
            return ErrorResponse.newBuilder().setMessage(msg).build();
        }
    }

    ResultColumnsMetaData createResultMetaData(ResultSetMetaData md) throws SQLException {
        List<ResultColumnMetaData> columns = new ArrayList<>();
        int nColumns = md.getColumnCount();
        for(int i=1; i<=nColumns; ++i) {
            columns.add(ResultColumnMetaData.newBuilder()
                .setOrdinal(i)
                .setName(md.getColumnName(i))
                .setType(md.getColumnType(i))
                .setSize(md.getColumnDisplaySize(i))
                .setNullable(md.isNullable(i))
                .setPrecision(md.getPrecision(i))
                .build());
        }
        return ResultColumnsMetaData.newBuilder().setColumns(columns).build();
    }

    List<Row> createRowData(ResultColumnsMetaData rowMetaData, ResultSet rs) throws SQLException {
        int nColumns = rowMetaData.getColumns().size();
        List<Row> data = new ArrayList<>();
        while (rs.next()) {
            var row = Row.newBuilder().setColumns(new ArrayList<>());
            for(int i=1; i<=nColumns; ++i) {
                var origValue = rs.getObject(i);
                var newValue = mapToType(origValue, rowMetaData.getColumns().get(i-1).getType());
                var col = BasicValue.newBuilder()
                        .setValue(newValue)
                        .build();
                row.getColumns().add(col);
            }
            data.add(row.build());

            if (data.size() > maxRows) {
                break;
            }
        }
        return data;
    }

    Object mapToType(Object value, int sqlType) {
        if (value == null) {
            return null;
        }

        switch(sqlType) {
            case Types.BOOLEAN:
                return value;

            case Types.BIT:
                // Weirdly, Postgres boolean fields are reported as having type java.sql.Types.BIT
                // but getObject(colindex) returns an instance of Boolean.
                if (value instanceof Boolean) {
                    return value;
                } else {
                    return ((Number) value).longValue();
                }

            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.BIGINT:
                return ((Number) value).longValue();

            case Types.FLOAT:
            case Types.DOUBLE:
                return ((Number) value).doubleValue();

            case Types.DECIMAL:
            case Types.NUMERIC:
                // AVRO doesn't have any equivalent of this. We could serialize it to bytes or to string.
                // The receiving end of course has to reverse this mapping...
                return value.toString();

            case Types.CHAR:
            case Types.VARCHAR:
                return value.toString();

            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                // Avro doesn't have native date/time types, so map these types to a String in ISO format
                if (value instanceof java.util.Date) {
                    // also handles java.sql.Date and java.sql.Timestamp
                    java.util.Date date = (java.util.Date) value;
                    var ldt = Instant.ofEpochMilli(date.getTime()).atZone(ZoneOffset.UTC).toLocalDateTime();
                    return DATE_TIME_FORMATTER.format(ldt);
                }
                if (value instanceof java.time.temporal.TemporalAccessor) {
                    return DATE_TIME_FORMATTER.format((java.time.temporal.TemporalAccessor) value);
                }
                return null;

            case Types.BLOB:
            case Types.CLOB:
                // Don't support these types for this auditproxy usecase
                return null;

            default:
                logger.warn("Unsupported type: {}", sqlType);
                return null;
        }
    }
}
