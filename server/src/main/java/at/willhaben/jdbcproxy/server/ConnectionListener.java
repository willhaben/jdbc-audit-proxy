package at.willhaben.jdbcproxy.server;

import at.willhaben.jdbcproxy.server.dao.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextStartedEvent;
import org.springframework.context.event.ContextStoppedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Listens for new TCP connections, and spawns a thread to handle requests to it.
 * These are not HTTP requests, so spring-web isn't used.
 */
@Component
public class ConnectionListener {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionListener.class);

    private final int maxRows;
    private final Duration socketLifetime;
    private final Map<String, Database> databases;
    private final Map<String, DataSource> dataSources;
    private final List<String> schemas;

    private final ServerSocket serverSocket;
    private final List<ConnectionHandler> handlers = new ArrayList<>(); // one per connected client
    private final Watchdog watchdog;
    private final SessionRepository sessionRepository;
    private final SessionManager sessionManager;
    private final Authenticator authenticator;
    private final AuditLogger auditLogger;

    private final AtomicBoolean done = new AtomicBoolean();

    ConnectionListener(ProxyConfig proxyConfig) throws IOException {
        maxRows = proxyConfig.getMaxRows();
        socketLifetime = Duration.parse(proxyConfig.getSocketLifetime());

        switch(proxyConfig.getAuthenticationManagement()) {
            case LOCAL:
                authenticator = new LocalAuthenticator(proxyConfig.getLocalAuth());
                break;

            case LDAP:
                authenticator = new LDAPAuthenticator(proxyConfig.getLDAPAuth());
                break;

            default:
                throw new UnsupportedOperationException();
        }

        Duration sessionLifetime = Duration.parse(proxyConfig.getSessionLifetime());

        databases = proxyConfig.getDatabases();

        var datasource = DatasourceProvider.createDataSource(proxyConfig.getAuditDb());
        if (datasource == null) {
            // use memory storage for sessions and logs for operations
            sessionRepository = new MemSessionRepository(sessionLifetime);
            auditLogger = new LoggingAuditLogger();
        } else {
            // use database storage for sessions and operations
            sessionRepository = new DBSessionRepository(sessionLifetime, datasource);
            auditLogger = new DBAuditLogger(databases, datasource, proxyConfig.getMaxCommandLength());
        }

        // It may be desirable for a configured datasource to have multiple schemas, but only allow some
        // of those to be browsed via this connection. A better solution, however, would be to use database
        // credentials which only has access to the desired schemas.
        // TODO: load the set of allowed schemas for this datasource from some configuration source
        schemas = null;

        sessionManager = new SessionManager(proxyConfig.getSessionManagement(), databases, sessionRepository, authenticator);

        // Create connection-pools for all databases that can be proxied to
        dataSources = createDataSources(databases);

        // Create socket for clients to connect to
        serverSocket = new ServerSocket(proxyConfig.getPort());

        // Ensure timeouts are enforced
        Duration queryLifetime = Duration.parse(proxyConfig.getQueryLifetime());
        watchdog = new Watchdog(queryLifetime);

        // Start listening for clients
        Thread t = new Thread(this::handleConnection);
        t.start();
    }

    /**
     * Create a set of DataSource objects, ie connection-pools wrapping a specific driver.
     * <p>
     * Don't create any connections; we don't want app startup to fail because some random database is down.
     * </p>
     */
    static Map<String, DataSource> createDataSources(Map<String, Database> databases) {
        // Create a pooling datasource for each entry in the databases map within the config.
        // Actually, pooling might be overkill for this proxy - but it's simple to add.
        return databases.entrySet().stream().collect(Collectors.toMap(
                e -> e.getKey(),
                e -> DatasourceProvider.createDataSource(e.getValue())));
    }

    @EventListener
    public void handleStartEvent(ContextStartedEvent event) {
        logger.info("Context Start Event received.");
    }

    @EventListener
    public void handleStopEvent(ContextStoppedEvent event) throws IOException {
        logger.info("Context Start Event received.");

        done.set(true);
        serverSocket.close(); // stop accepting new connections - and also wakes code blocked on serverSocket.accept()
        watchdog.expireAll(); // kill all currently running SQL requests (triggering error messages back to client)
        handlers.forEach(ConnectionHandler::stop); // close all existing connections from clients
    }

    void handleConnection() {
        try {
            while (!done.get()) {
                Socket socket = serverSocket.accept();
                ConnectionHandler handler = new ConnectionHandler(
                        maxRows,
                        schemas,
                        sessionManager,
                        socket,
                        socketLifetime,
                        databases,
                        dataSources,
                        watchdog,
                        auditLogger,
                        authenticator);
                handlers.add(handler);
                Thread t = new Thread(handler);
                t.start();
            }
        } catch(IOException e) {
            // ignore
           logger.warn("Failed to accept socket");
        }
    }
}