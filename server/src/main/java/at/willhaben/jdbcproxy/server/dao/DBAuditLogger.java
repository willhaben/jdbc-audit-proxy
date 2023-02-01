package at.willhaben.jdbcproxy.server.dao;

import at.willhaben.jdbcproxy.server.Database;
import at.willhaben.jdbcproxy.server.model.Login;
import at.willhaben.jdbcproxy.server.model.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of AuditLogger which writes info to database tables.
 * <p>
 * This allows an audit-trail of db-connections and sql-commands to be retrieved via SQL later - either
 * directly, or via the JDBCAuditProxy itself (if so configured).
 * </p>
 * <p>
 * A database can be marked as "not audited", in which case sessions and logins are still tracked, but
 * individual operations are not.
 * </p>
 */
public class DBAuditLogger implements AuditLogger {
    private static final Logger logger = LoggerFactory.getLogger(DBAuditLogger.class);

    private static final String COLUMN_SESSION = "session";
    private static final String COLUMN_AT = "at";
    private static final String COLUMN_COMMAND = "command";

    private final Map<String, Database> databases;
    private final JdbcTemplate jdbcTemplate;
    private final int maxCommandLength;

    public DBAuditLogger(Map<String, Database> databases, DataSource dataSource, int maxCommandLength) throws IOException {
        this.databases = databases;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.maxCommandLength = maxCommandLength;

        validateMaxCommandLength(dataSource, maxCommandLength);
    }

    @Override
    public Login recordLogin(Session session) throws IOException {
        try {
            var login = new Login(session);

            SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate)
                    .withTableName("LOGIN")
                    .usingColumns(COLUMN_SESSION, COLUMN_AT);

            var cols = new HashMap<String, Object>();
            cols.put(COLUMN_SESSION, session.getId());
            cols.put(COLUMN_AT, new java.sql.Timestamp(login.getAt().toEpochMilli()));
            simpleJdbcInsert.execute(cols);

            return login;
        } catch (RuntimeException e) {
            throw new IOException("Unable to store login", e);
        }
    }

    private boolean isAudited(String database) {
        var db = databases.get(database);
        if (db == null) {
            return true;
        }
        return db.isAudited();
    }

    @Override
    public void recordOperation(Login login, String operation) throws IOException {
        if (!isAudited(login.getSession().getDatabase())) {
            return;
        }

        if (operation.length() >= maxCommandLength) {
            // truncating the operation doesn't seem safe: the audit trail would have incomplete info.
            // Executing the SQL will fail, but the error-message won't be very helpful. So fail here
            // with an explicit message.
            throw new IOException("Unable to record operation: longer than max of " + maxCommandLength);
        }

        try {
            SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate)
                    .withTableName("OPERATION")
                    .usingColumns(COLUMN_SESSION, COLUMN_AT, COLUMN_COMMAND);

            var at = Instant.now(); // operation-at, not login-at

            var cols = new HashMap<String, Object>();
            cols.put(COLUMN_SESSION, login.getSession().getId());
            cols.put(COLUMN_AT, new java.sql.Timestamp(at.toEpochMilli()));
            cols.put(COLUMN_COMMAND, operation);
            simpleJdbcInsert.execute(cols);
        } catch (RuntimeException e) {
            throw new IOException("Unable to store operation", e);
        }
    }

    // ====  static helper methods

    /**
     * Use database metadata to determine the longest command that can be stored, and block startup
     * if the configured value is larger than what the database can actually store.
     */
    private static void validateMaxCommandLength(DataSource dataSource, int maxCommandLength) throws IOException {
        int maxFromDB;
        try {
            maxFromDB = getMaxCommandLengthFromDB(dataSource);
        } catch(IOException e) {
            // Ignore and continue. We don't want this proxy to fail to start up just because its
            // database isn't currently available - that leads to service startup ordering concerns.
            // Failing when config is wrong is helpful; failing when it _might_ be wrong is not.
            // If the config is wrong, then inserts will fail later anyway - just with a less
            // helpful error message.
            logger.warn("Unable to validate max command length");
            return;
        }

        if (maxFromDB < maxCommandLength) {
            String msg = String.format(
                    "Commands configured with max length of %d but database supports only %d chars",
                    maxCommandLength, maxFromDB);
            logger.error(msg);
            throw new IOException(msg);
        }
    }

    /**
     * Use database metadata to determine the longest command that can be stored.
     */
    private static int getMaxCommandLengthFromDB(DataSource dataSource) throws IOException {
        try (var conn = dataSource.getConnection()) {
            try (var rs = conn.getMetaData().getColumns(null, null, "operation", "command")) {
                rs.next();
                return rs.getInt(7);
            }
        } catch(SQLException e) {
            throw new IOException(e);
        }
    }
}
