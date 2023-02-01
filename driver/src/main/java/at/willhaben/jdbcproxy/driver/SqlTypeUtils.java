package at.willhaben.jdbcproxy.driver;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Static helpers related to manipulating SQL types.
 */
public class SqlTypeUtils {
    private static final Map<Integer, String> sqlTypeNames = makeSqlTypeNamesMap();

    private static Map<Integer, String> makeSqlTypeNamesMap() {
        Map<Integer, String> map = new HashMap<>();
        map.put(Types.VARCHAR, "varchar");
        map.put(Types.INTEGER, "integer");
        map.put(Types.BIGINT, "bigint");
        map.put(Types.SMALLINT, "smallint");
        map.put(Types.TINYINT, "tinyint");
        map.put(Types.BOOLEAN, "boolean");
        map.put(Types.DATE, "date");
        map.put(Types.TIME, "time");
        map.put(Types.TIMESTAMP, "timestamp");
        map.put(Types.BIT, "bit*");
        map.put(Types.BLOB, "blob*");
        map.put(Types.DECIMAL, "decimal");
        map.put(Types.DOUBLE, "double");
        map.put(Types.FLOAT, "float");
        map.put(Types.CHAR, "char");
        map.put(Types.NUMERIC, "numeric");
        return Collections.unmodifiableMap(map);
    }

    /**
     * Provide human-friendly names for SQL column types.
     * <p>
     * Normally, this info comes from the target database via the DatabaseMetaData, but the proxy
     * doesn't bother to transfer this map of data via the network message; instead use "generic"
     * names for types here, which should be good enough.
     * </p>
     */
    public static String toTypeName(int sqlType) {
        var name = sqlTypeNames.get(sqlType);
        return (name == null) ? "unknown" : name;
    }

    private SqlTypeUtils() {

    }

}
