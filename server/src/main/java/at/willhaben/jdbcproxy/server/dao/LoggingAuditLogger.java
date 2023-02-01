package at.willhaben.jdbcproxy.server.dao;

import at.willhaben.jdbcproxy.server.model.Login;
import at.willhaben.jdbcproxy.server.model.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of AuditLogger which just writes text to an SLF4J Logger object.
 * <p>
 * This allows an audit-trail of db-connections and sql-commands to be stored in a file, or
 * centralized in some logmessage-manager such as Kibana/Splunk/etc.
 * </p>
 */
public class LoggingAuditLogger implements AuditLogger {
    private static final Logger logger = LoggerFactory.getLogger(LoggingAuditLogger.class);

    @Override
    public Login recordLogin(Session session) {
        logger.info("Connection authorized: user={} database={}", session.getUsername(), session.getDatabase());
        return new Login(session);
    }

    @Override
    public void recordOperation(Login login, String operation) {
        // The current time (at which operation was executed) is expected to be
        // implicitly added via the logger-format-string.
        String msg = String.format(
                "user=%s: connectedAt=%s query=[%s]",
                login.getSession().getUsername(),
                login.getAt(),
                operation);
        logger.info(msg);
    }
}
