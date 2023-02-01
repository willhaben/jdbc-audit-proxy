package at.willhaben.jdbcproxy.driver;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A jdbc ResultSet implementation that returns data from an in-memory collection.
 * <p>
 * This is implemented as a dynamic InvocationHandler rather than implementing interface ResultSet because
 * that interface has a very large number of methods, most of which are irrelevant for the use-cases that
 * this driver is used for.
 * </p>
 */
class ProxyMemResultSet implements InvocationHandler {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private final ProxyResultSetMetaData metaData;
    private final List<List<?>> values;
    private int rowNum = -1;
    private boolean wasNull;

    // Cache of (colname->colindex) for the metadata of this resultset; computed on demand.
    private Map<String, Integer> colNameToIndex = null;

    /**
     * Factory for an empty result-set with the specified metadata-format.
     */
    static ResultSet of(ProxyResultSetMetaData metaData) {
        return of(metaData, List.of());
    }

    /**
     * Factory for a result-set with the specified metadata-format and specified content.
     */
    static ResultSet of(ProxyResultSetMetaData metaData, List<List<?>> rowData) {
        return (ResultSet) Proxy.newProxyInstance(
                ProxyMemResultSet.class.getClassLoader(),
                new Class[]{ResultSet.class},
                new ProxyMemResultSet(metaData, rowData));
    }

    private ProxyMemResultSet(ProxyResultSetMetaData metaData, List<List<?>> values) {
        this.metaData = metaData;
        this.values = values;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if ("getMetaData".equals(methodName)) {
            return metaData;
        }

        if ("getType".equals(methodName)) {
            return ResultSet.TYPE_FORWARD_ONLY;
        }

        if ("next".equals(methodName)) {
            if (rowNum == Integer.MAX_VALUE) {
                throw new IllegalStateException(); // closed
            }
            rowNum = rowNum + 1;
            return rowNum < values.size();
        }

        if ("getRow".equals(methodName)) {
            return rowNum + 1;
        }

        if ("isBeforeFirst".equals(methodName)) {
            return (rowNum < 0);
        }

        if ("isAfterLast".equals(methodName)) {
            return (rowNum == values.size());
        }

        if ("isFirst".equals(methodName)) {
            return (rowNum == 0);
        }

        if ("isLast".equals(methodName)) {
            return (rowNum == values.size() - 1);
        }

        if ("close".equals(methodName)) {
            rowNum = Integer.MAX_VALUE;
            return null;
        }

        if ("isClosed".equals(methodName)) {
            return (rowNum == Integer.MAX_VALUE);
        }

        if ("wasNull".equals(methodName)) {
            return wasNull; // true iff getSomeType(col) recently returned null
        }

        if ("getString".equals(methodName))
            return getColumnOfCurrentRow(method, args);

        if ("getBoolean".equals(methodName))
            return getColumnOfCurrentRow(method, args);

        if ("getBigDecimal".equals(methodName)) {
            // DECIMAL and NUMERIC are transmitted from the server as strings
            var value = getColumnOfCurrentRow(method, args);
            return (value == null) ? null : new BigDecimal(value.toString());
        }

        if ("getLong".equals(methodName)) {
            var value = getColumnOfCurrentRow(method, args);
            return (value == null) ? 0 : ((Number) value).longValue();
        }

        if ("getInt".equals(methodName)) {
            var value = getColumnOfCurrentRow(method, args);
            return (value == null) ? 0 : ((Number) value).intValue();
        }

        if ("getShort".equals(methodName)) {
            var value = getColumnOfCurrentRow(method, args);
            return (value == null) ? 0 : ((Number) value).shortValue();
        }

        if ("getByte".equals(methodName)) {
            var value = getColumnOfCurrentRow(method, args);
            return (value == null) ? 0 : ((Number) value).byteValue();
        }

        if ("getDate".equals(methodName))
            return getColumnOfCurrentRow(method, args);

        if ("getTime".equals(methodName))
            return getColumnOfCurrentRow(method, args);

        if ("getTimestamp".equals(methodName))
            return getColumnOfCurrentRow(method, args);

        if ("getObject".equals(methodName)) {
            if (method.getParameterTypes().length != 1) {
                // getObject has some advanced versions eg getObject(int,  typemap)
                throw new UnsupportedOperationException("ResultSet.getObject with multiple args");
            }

            return getColumnOfCurrentRow(method, args);
        }

        if ("getWarnings".equals(methodName)) {
            return null;
        }

        if ("clearWarnings".equals(methodName)) {
            return null;
        }

        if ("findColumn".equals(methodName)) {
            return findColumn((String) args[0]);
        }

        if ("beforeFirst".equals(methodName) || "afterLast".equals(methodName)
                || "first".equals(methodName) || "last".equals(methodName)) {
            throw new SQLException(String.format(
                    "Method %s not valid for ResultSet of type-forward-only", methodName));
        }

        if ("getFetchSize".equals(methodName)) {
            return 100;
        }

        if ("setFetchSize".equals(methodName)) {
            return null; // ignore
        }

        throw new UnsupportedOperationException("ProxyMemResultSet." + methodName);
    }

    List<?> getCurrentRow() throws SQLException {
        if ((rowNum < 0) || (rowNum >= values.size())) {
            throw new SQLException("illegal row-index in fake resultset");
        }
        return values.get(this.rowNum);
    }

    Object getColumnOfCurrentRow(int column) throws SQLException {
        if ((column < 1) || (column > metaData.getColumnCount())) {
            var msg = String.format(
                    "illegal column in fake resultset for table %s: %d/%d",
                    metaData.getTableName(),
                    column,
                    metaData.getColumnCount());
            throw new SQLException(msg);
        }

        var row = getCurrentRow();

        if (column > row.size()) {
            // This mem-resultset doesn't always have a value for every trailing column; just
            // return null for those "unpopulated columns".
            return null;
        }

        return row.get(column-1);
    }

    Object getColumnOfCurrentRow(Method method, Object[] args) throws SQLException {
        int idx = getColumnIndexOfCurrentRow(method, args);
        var value = getColumnOfCurrentRow(idx);
        if (value == null) {
            this.wasNull = true;
            return null;
        }
        this.wasNull = false;
        return mapType(metaData.getColumnType(idx), value);
    }

    int getColumnIndexOfCurrentRow(Method method, Object[] args) throws SQLException {
        var arg0Type = method.getParameterTypes()[0];

        if (arg0Type == Integer.TYPE) {
            // caller invoked something like resultSet.getString(1)
            return (int) args[0];
        }

        if (arg0Type == String.class) {
            // caller invoked something like resultSet.getString("col1name")
            return findColumn((String) args[0]);
        }

        throw new UnsupportedOperationException("Unable to get column by specifier of type " + arg0Type);
    }

    private Map<String, Integer> computeColNameToIndexMap(ResultSetMetaData metaData) throws SQLException {
        Map<String, Integer> map = new HashMap<>();
        for(int idx = 1; idx <= metaData.getColumnCount(); ++idx) {
            map.put(metaData.getColumnName(idx), idx);
        }
        return map;
    }

    int findColumn(String colName) throws SQLException {
        if (colNameToIndex == null) {
            colNameToIndex = computeColNameToIndexMap(this.metaData);
        }
        var idx = colNameToIndex.get(colName);
        if (idx == null) {
            throw new SQLException("No such column: " + colName);
        }
        return idx;
    }

    // ProxyStatement.toResultSetRows just builds the row-data using the raw types from the AVRO message.
    // However there isn't a perfect 1:1 match between JDBC types and AVRO types, so here we do any necessary
    // conversions...
    Object mapType(int coltype, Object value) {
        switch(coltype) {
            case Types.DATE:
                return toDate(value.toString(), java.sql.Date::new);

            case Types.TIME:
                return toDate(value.toString(), java.sql.Time::new);

            case Types.TIMESTAMP:
                return toDate(value.toString(), java.sql.Timestamp::new);

            default:
                return value;
        }
    }

    // AVRO doesn't have a "date" type, so these types are passed as strings in ISO format; here
    // we convert that back to the appropriate type.
    java.util.Date toDate(String src, Function<Long, Date> constructor) {
        var instant = DATE_TIME_FORMATTER.parse(src, LocalDateTime::from)
                .atOffset(ZoneOffset.UTC).toInstant();
        return constructor.apply(instant.toEpochMilli());
    }
}
