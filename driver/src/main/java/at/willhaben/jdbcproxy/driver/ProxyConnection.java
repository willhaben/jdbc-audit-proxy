package at.willhaben.jdbcproxy.driver;

import at.willhaben.jdbcproxy.server.avro.MetaDataRequest;
import at.willhaben.jdbcproxy.server.avro.MetaDataResponse;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.Properties;

/**
 * Emulate the JDBC Connection class, ie generally manage network connectivity to the remote system,
 * provide basic metadata about the remote database, and act as a factory for JDBC Statement objects.
 * <p>
 * This is implemented as a dynamic InvocationHandler rather than implementing interface Connection because
 * that interface has a very large number of methods, most of which are irrelevant for the use-cases that
 * this driver is used for.
 * </p>
 */
public class ProxyConnection implements InvocationHandler {
    private static final ClassLoader connectionClassLoader = Connection.class.getClassLoader();
    private static final Class[] connectionInterfaces = new Class[] { Connection.class };

    private static final Properties EMPTY_PROPERTIES = new Properties();

    private final Communicator communicator;
    private ProxyDatabaseMetaData proxyDatabaseMetaData;

    public static Connection of(Communicator communicator) {
        return (Connection) Proxy.newProxyInstance(
                connectionClassLoader,
                connectionInterfaces,
                new ProxyConnection(communicator));
    }

    public ProxyConnection(Communicator communicator) {
        this.communicator = communicator;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Exception {
        String methodName = method.getName();

        if ("getMetaData".equals(methodName)) {
            // Need to cache as at least some tools call this method repeatedly during startup.
            // The proxy server returns *all* available data in the MetaDataResponse object so there
            // is no point re-fetching it (except possibly to "refresh" - but we just don't support that
            // except by disconnect and reconnect).
            if (proxyDatabaseMetaData == null) {
                var request = new MetaDataRequest();
                MetaDataResponse response = communicator.send(request, MetaDataResponse.class);
                proxyDatabaseMetaData =  ProxyDatabaseMetaData.of(response);
            }
            return proxyDatabaseMetaData;
        }

        if ("getCatalog".equals(methodName)) {
            // proxy doesn't support multiple catalogs/schemas.
            return "default";
        }

        if ("createStatement".equals(methodName)) {
            // return an object which gathers sql and params then on executeQuery sends a message
            return ProxyStatement.of(null, communicator);
        }

        if ("prepareStatement".equals(methodName)) {
            // return an object which gathers sql and params then on executeQuery sends a message
            throw new UnsupportedOperationException("Connection.prepareStatement");
        }

        if ("close".equals(methodName)) {
            communicator.close();
            return null;
        }

        if ("isClosed".equals(methodName)) {
            return communicator.isClosed();
        }

        if ("getAutoCommit".equals(methodName)) {
            return true;
        }

        if ("getSchema".equals(methodName)) {
            return "default";
        }

        if ("isValid".equals(methodName)) {
            // TODO: send a "ping" packet to the server
            return true;
        }

        if ("getWarnings".equals(methodName)) {
            return null;
        }

        if ("clearWarnings".equals(methodName)) {
            return null;
        }

        if ("setReadOnly".equals(methodName)) {
            return null;
        }

        if ("setCatalog".equals(methodName)) {
            return null;
        }

        if ("getClientInfo".equals(methodName)) {
            return EMPTY_PROPERTIES;
        }

        if ("setClientInfo".equals(methodName)) {
            return null;
        }

        // anything else
        throw new UnsupportedOperationException("Connection." + methodName);
    }
}
