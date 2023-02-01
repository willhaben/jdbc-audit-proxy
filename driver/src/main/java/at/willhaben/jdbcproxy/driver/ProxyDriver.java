package at.willhaben.jdbcproxy.driver;

import at.willhaben.jdbcproxy.server.avro.AuthenticationRequest;
import at.willhaben.jdbcproxy.server.avro.AuthenticationResponse;

import java.net.Socket;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Implement the standard JDBC Driver interface so that this client-side code can be loaded by
 * normal JDBC-enabled applications - and in particular, DB GUI applications which can be used
 * to submit SQL statements to a database and display the results nicely.
 * <p>
 * JDBC connection parameter "timeout_secs" can be used to specify how long the connection
 * to the server should be kept open for - including "0" for no timeout.
 * </p>
 */
@SuppressWarnings("unused")
public class ProxyDriver implements Driver {
    public static final String URL_NAMESPACE = "jdbc:auditproxy:";
    public static final String URL_PREFIX = URL_NAMESPACE + "//";

    // max time that any communication to the server can take.
    private static final int SOCKET_TIMEOUT_SECS_DFLT = 60;

    // jdbc URL query-parameter that can override the default timeout
    private static final String SOCKET_TIMEOUT_PARAM = "timeout_secs";

    static class Target {
        private final String host;
        private final int port;
        private final String database;
        private final Map<String,String> params;

        Target(String host, int port, String database, Map<String,String> params) {
            this.host = host;
            this.port = port;
            this.database = database;
            this.params = params;
        }
    }

    @Override
    @SuppressWarnings("squid:S2095") // socket is closed by caller, not in this method
    public Connection connect(String url, Properties info) throws SQLException {
        Target target = parseUrl(url);

        try {
            Socket socket = new Socket(target.host, target.port);
            socket.setSoTimeout(1000 *  getSocketTimeoutSecs(target.params));

            Communicator communicator = new Communicator(socket);
            var request = AuthenticationRequest.newBuilder()
                    .setUsername(info.getProperty("user"))
                    .setPassword(info.getProperty("password"))
                    .setDb(target.database)
                    .build();
            var response = communicator.send(request, AuthenticationResponse.class);
            if (!response.getAccepted()) {
                throw new SQLException("Authorization failed:" + response.getErrorMessage().orElse("Unknown error"));
            }
            var conn = ProxyConnection.of(communicator);
            startKeepAlive(communicator);
            return conn;
        } catch(SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Unable to connect to server", e);
        }
    }

    private int getSocketTimeoutSecs(Map<String,String> jdbcParams) {
        var timeoutParam = jdbcParams.get(SOCKET_TIMEOUT_PARAM);
        if (timeoutParam != null) {
            return Integer.parseInt(timeoutParam);
        }
        return SOCKET_TIMEOUT_SECS_DFLT;
    }

    /**
     * Start a background thread that sends "keepalive" messages to the server to prevent the client/server
     * socket from being closed by infrastructure.
     * <p>
     * The thread terminates automatically when the connection to the server is closed. The thread is also
     * marked as "daemon" so it terminates when the application terminates.
     * </p>
     */
    private void startKeepAlive(Communicator communicator) {
        var keepAlive = new KeepAlive(communicator);
        var thread = new Thread(keepAlive);
        thread.setDaemon(true);
        thread.start();
    }

    // Possibly a suitable regex could extract all the relevant info from the URL in one go,
    // but it would be fairly complex..
    private Target parseUrl(String url) {
        // jdbc:auditproxy://host:port/database?params
        // - where host:port is the address of the proxy
        // - and database is one of the configured backend DBs for the proxy
        if (!url.startsWith(URL_PREFIX)) {
            throw new IllegalArgumentException(invalidUrl(url));
        }

        String base = url.substring(URL_PREFIX.length()); // host:port/database?params
        var baseParts = base.split("/");
        if (baseParts.length != 2) {
            throw new IllegalArgumentException(invalidUrl(url));
        }

        var hostAndPortParts = baseParts[0].split(":");
        if (hostAndPortParts.length != 2) {
            throw new IllegalArgumentException(invalidUrl(url));
        }

        String host = hostAndPortParts[0];
        int port;
        try {
            port = Integer.parseInt(hostAndPortParts[1]);
        } catch(NumberFormatException e) {
            throw new IllegalArgumentException(invalidUrl(url));
        }

        var dbAndParamsParts = baseParts[1].split("\\?");
        String db = dbAndParamsParts[0];

        Map<String, String> params = (dbAndParamsParts.length == 1) ? Map.of() : paramsToMap(url, dbAndParamsParts[1]);

        return new Target(host, port, db, params);
    }

    // convert "foo=foo1&bar=bar1" into a map..
    private Map<String, String> paramsToMap(String url, String params) {
        // split on "&"
        // for each, split on "="
        var items = params.split("&");
        Map<String,String> result = new HashMap<>();
        for(var item : items) {
            var parts = item.split("=");
            if (parts.length != 2) {
                throw new IllegalArgumentException(invalidUrl(url));
            }
            result.put(parts[0], parts[1]);
        }
        return result;
    }

    private String invalidUrl(String url) {
        return String.format("invalid url: %s", url);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(URL_NAMESPACE);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
