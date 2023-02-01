package at.willhaben.jdbcproxy.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Terminates a SQL statement or network socket after a specific period of time.
 */
public class Watchdog {
    private static final Logger logger = LoggerFactory.getLogger(Watchdog.class);

    public class StmtWatch implements AutoCloseable {
        final PreparedStatement stmt;
        final long expiresAt;
        final String user;

        StmtWatch(PreparedStatement stmt, long expiresAt, String user) {
            this.stmt = stmt;
            this.expiresAt = expiresAt;
            this.user = user;
        }

        public void close() {
            stmtWatches.remove(this);
        }

        void expire() {
            if (stmtWatches.remove(this)) {
                logger.warn("Force-cancelling SQL statement for client {}", user);
                try {
                    stmt.cancel();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    public class SocketWatch implements AutoCloseable {
        final Socket socket;
        final long expiresAt;
        String user = "unknown";

        SocketWatch(Socket socket, long expiresAt) {
            this.socket = socket;
            this.expiresAt = expiresAt;
        }

        // For sockets, we don't initially know who the remote user is (authentication occurs only after
        // socket creation). Therefore allow the username to be updated once it is known..
        public void setUsername(String user) {
            this.user = user;
        }

        public void close() {
            socketWatches.remove(this);
        }

        public void expire() {
            if (socketWatches.remove(this)) {
                logger.warn("Force-closing socket for client {}", user);
                try {
                    socket.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    private static final Duration SCAN_INTERVAL = Duration.ofSeconds(30);

    private final Duration queryLifetime;
    private final Set<StmtWatch> stmtWatches = Collections.synchronizedSet(new HashSet<>());
    private final Set<SocketWatch> socketWatches = Collections.synchronizedSet(new HashSet<>());
    private final Thread expiryThread;

    public Watchdog(Duration queryLifetime) {
        this.queryLifetime = queryLifetime;

        expiryThread = new Thread(this::watch);
        expiryThread.setDaemon(true); // don't block application shutdown...
        expiryThread.start();
    }

    /**
     * Loop forever (or until explicitly interrupted), terminating SQL-statements and network-sockets
     * which have passed their associated lifetimes.
     */
    @SuppressWarnings("squid:S2189") // suppress sonarqube: this daemon thread does not need to exit
    private void watch() {
        try {
            while (true) {
                scanWatches();
                Thread.sleep(SCAN_INTERVAL.toMillis());
            }
        } catch(InterruptedException e) {
            // re-raise as per normal practice - which in effect causes this thread to terminate.
            Thread.currentThread().interrupt();
        } finally {
            logger.warn("Watchdog terminating");
        }
    }

    /**
     * Scan all existing statements and sockets, closing them if needed.
     * <p>
     * This uses a pretty brute-force way to find expired items. However the list of currently-running
     * statements and currently-open sockets is expected to be very small.
     * </p>
     */
    private void scanWatches() {
        long now = System.currentTimeMillis();

        var expiredStatements = stmtWatches.stream()
                .filter(e -> e.expiresAt < now)
                .toList();
        expiredStatements.forEach(StmtWatch::expire);

        var expiredSockets = socketWatches.stream()
                .filter(e -> e.expiresAt < now)
                .toList();
        expiredSockets.forEach(SocketWatch::expire);
    }

    /**
     * Run a query with a maximum duration (stmt.cancel will be called after this interval).
     */
    public ResultSet executeQuery(PreparedStatement statement, String user) throws SQLException {
        var stmtWatch = new StmtWatch(statement, System.currentTimeMillis() + queryLifetime.toMillis(), user);
        stmtWatches.add(stmtWatch);
        try {
            return statement.executeQuery();
        } finally {
            stmtWatches.remove(stmtWatch);
        }
    }

    public SocketWatch closeAfter(Socket socket, Duration maxDuration) {
        SocketWatch watch = new SocketWatch(socket, System.currentTimeMillis() + maxDuration.toMillis());
        socketWatches.add(watch);
        return watch;
    }

    public void expireAll() {
        // not actually needed, as this is a daemon thread
        expiryThread.interrupt();

        stmtWatches.forEach(StmtWatch::expire);
        socketWatches.forEach(SocketWatch::expire);
    }
}
