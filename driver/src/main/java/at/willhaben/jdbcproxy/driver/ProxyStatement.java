package at.willhaben.jdbcproxy.driver;

import at.willhaben.jdbcproxy.server.avro.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Emulate the JDBC Statement class, ie store parameters for queries as they are defined then submit a
 * request to the remote server when the statement is executed.
 * <p>
 * This is implemented as a dynamic InvocationHandler rather than implementing interface Statement because
 * that interface has a very large number of methods, most of which are irrelevant for the use-cases that
 * this driver is used for.
 * </p>
 */
public class ProxyStatement implements InvocationHandler {
    private static final ClassLoader statementClassLoader = Statement.class.getClassLoader();
    private static final Class[] statementInterfaces = new Class[] { Statement.class };

    private final Connection connection;
    private final Communicator communicator;
    private ResultSet results = null;
    private int maxRows = 0;
    private boolean isClosed;
    private String warning;

    public static Statement of(Connection connection, Communicator communicator) {
        return (Statement) Proxy.newProxyInstance(
                statementClassLoader,
                statementInterfaces,
                new ProxyStatement(connection, communicator));
    }

    ProxyStatement(Connection connection, Communicator communicator) {
        this.connection = connection;
        this.communicator = communicator;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (isClosed) {
            throw new SQLException("Statement is closed");
        }

        if ("close".equals(methodName)) {
            isClosed = true;
            return null;
        }

        if ("isClosed".equals(methodName)) {
            return isClosed;
        }

        if ("execute".equals(methodName)) {
            try {
                results = doExecute(method, args);
                warning = null; // as per spec, previous warnings are cleared on execute
                return true; // results are present
            } catch(ProxyError e) {
                results = null;
                warning = e.getMessage();
                return false; // results are not present
            }
        }

        if ("executeQuery".equals(methodName)) {
            // like execute() except that the returned value is the resultSet, not just "isPresent"
            try {
                results = doExecute(method, args);
                warning = null; // as per spec, previous warnings are cleared on execute
                return results;
            } catch(ProxyError e) {
                results = null;
                warning = e.getMessage();
                throw new SQLException(e);
            }
        }

        if ("getResultSet".equals(methodName)) {
            if (results == null) {
                throw new SQLException("No result available");
            }
            return results;
        }

        if ("getConnection".equals(methodName)) {
            // ensure proxying is retained
            return connection;
        }

        if ("setMaxRows".equals(methodName)) {
            maxRows = (int) args[0];
            return null;
        }

        if ("getMaxRows".equals(methodName)) {
            return maxRows;
        }

        if ("getWarnings".equals(methodName)) {
            return (warning == null) ? null : new SQLWarning(warning);
        }

        if ("clearWarnings".equals(methodName)) {
            warning = null;
            return null;
        }

        if ("getUpdateCount".equals(methodName)) {
            return -1;
        }

        if ("setFetchSize".equals(methodName)) {
            return null;
        }

        if ("getMoreResults".equals(methodName)) {
            // only one result-set per query is supported
            if (results == null) {
                return false;
            }

            results.close();
            results = null;
            return false;
        }

        if ("setEscapeProcessing".equals(methodName)) {
            boolean state = (boolean) args[0];
            if (state == true) {
                // hmm .. not really safe to accept this as we don't have any way to tell the server
                // to please do escape-processing. But allow it for now
                FileLog.log("Warning: client trying to enable escape-processing");
            }
            return null;
        }

        throw new UnsupportedOperationException("ProxyStatement." + methodName);
    }

    ResultSet doExecute(Method method, Object[] args) throws ProxyError, ConnectionClosedError {
        if (method.getParameterTypes().length == 0) {
            throw new UnsupportedOperationException("Statement.execute()");
        }

        var ptype = method.getParameterTypes()[0];
        if (ptype != String.class) {
            // only execute(string) currently supported
            throw new UnsupportedOperationException(String.format("Statement.execute(%s)", ptype.getName()));
        }

        // Serialize this object to a SelectRequest and send it over the socket
        // then read the SelectResponse and create a ResultSet wrapper around it
        //
        // Note that the resulting objects can be fetched via method getResultSet() and getMoreResults()
        String query = (String) args[0];
        var response = communicator.send(
                SelectRequest.newBuilder().setQuery(query).build(),
                SelectResponse.class);
        var metadata = toResultSetMetaData("results", response.getMetadata());
        var rows = toResultSetRows(response.getRows());
        return ProxyMemResultSet.of(metadata, rows);
    }

    ProxyResultSetMetaData toResultSetMetaData(String tableName, ResultColumnsMetaData rowMetaData) {
        var colNames = rowMetaData.getColumns().stream().map(c -> c.getName()).collect(Collectors.toUnmodifiableList());
        var colTypes = rowMetaData.getColumns().stream().map( c -> c.getType()).collect(Collectors.toUnmodifiableList());
        var colNullable = rowMetaData.getColumns().stream().map (c -> c.getNullable()).collect(Collectors.toUnmodifiableList());
        return new ProxyResultSetMetaData(tableName, colNames, colTypes, colNullable);
    }

    List<List<?>> toResultSetRows(List<Row> rows) {
        // Note that the returned lists contains types exactly as received in the AVRO message. Any conversion
        // to other types is done in ProxyMemResultSet.mapType(..)
        return rows.stream().map(r -> {
            var columns = r.getColumns().stream()
                    .map(col -> col.getValue().orElse(null))
                    .collect(Collectors.toList());
            return columns;
        }).collect(Collectors.toUnmodifiableList());
    }
}
