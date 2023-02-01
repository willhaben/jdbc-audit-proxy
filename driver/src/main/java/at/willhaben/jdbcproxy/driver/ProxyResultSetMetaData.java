package at.willhaben.jdbcproxy.driver;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * A trivial implementation of ResultSetMetaData, ie an object that describes the "shape" of a resultset
 * in terms of (column-name, column-type) plus some extra data.
 */
public class ProxyResultSetMetaData implements ResultSetMetaData {
    private final String tableName;
    private final List<String> colNames;
    private final List<Integer> colTypes; // see java.sql.Types
    private final List<Integer> colNullables;

    public ProxyResultSetMetaData(String tableName, List<String> colNames, List<Integer> colTypes) {
        this(tableName, colNames, colTypes, null);
    }

    public ProxyResultSetMetaData(
            String tableName,
            List<String> colNames,
            List<Integer> colTypes,
            List<Integer> colNullables) {
        this.tableName = tableName;
        this.colNames = colNames;
        this.colTypes = colTypes;
        this.colNullables = colNullables;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return colTypes.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return (colNullables == null) ? 0 : colNullables.get(column - 1);
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 10;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return colNames.get(column - 1);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return colNames.get(column - 1);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "default";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return tableName;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "default";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return colTypes.get(column-1);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        var colType = getColumnType(column);
        return SqlTypeUtils.toTypeName(colType);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return String.class.getCanonicalName(); // TODO use a map for this
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("ResultSetMetaData.unwrap");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
