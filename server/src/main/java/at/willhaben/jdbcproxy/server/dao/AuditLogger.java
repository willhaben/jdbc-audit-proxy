package at.willhaben.jdbcproxy.server.dao;

import at.willhaben.jdbcproxy.server.model.Login;
import at.willhaben.jdbcproxy.server.model.Session;

import java.io.IOException;

/**
 * Service that can record audit-relevant data for future analysis.
 */
public interface AuditLogger {
    /**
     * Record that a specific user has connected to a specific database.
     * <p>
     * Note that a user may connect to a database multiple times using the same "logical session" (as long
     * as it has not expired).
     * </p>
     */
    Login recordLogin(Session session) throws IOException;

    /**
     * Record the actual SQL statement that a user has executed.
     */
    void recordOperation(Login login, String operation) throws IOException;
}
