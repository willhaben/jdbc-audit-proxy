package at.willhaben.jdbcproxy.server;

import at.willhaben.jdbcproxy.server.avro.AuthenticationRequest;
import at.willhaben.jdbcproxy.server.avro.SessionRequest;
import at.willhaben.jdbcproxy.server.dao.SessionException;
import at.willhaben.jdbcproxy.server.dao.SessionRepository;
import at.willhaben.jdbcproxy.server.model.Session;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

public class SessionManager {
    private final SessionManagement defaultSessionManagement;
    private final Map<String, Database> databases;
    private final SessionRepository sessionRepository;
    private final Authenticator authenticator;

    public SessionManager(
            SessionManagement defaultSessionManagement,
            Map<String, Database> databases,
            SessionRepository sessionRepository,
            Authenticator authenticator) {
        this.defaultSessionManagement = defaultSessionManagement;
        this.databases = databases;
        this.sessionRepository = sessionRepository;
        this.authenticator = authenticator;
    }

    /**
     * Create an explicitly-requested session.
     */
    public int createSession(SessionRequest request) throws IOException, SessionException {
        var username = request.getUsername();
        var password = request.getPassword();
        var targetUser = request.getForUsername().orElse(username);
        var targetDB = request.getDb();

        if (defaultSessionManagement == SessionManagement.APPROVED) {
            if (username.equals(targetDB)) {
                throw new IOException("SessionManagement mode is APPROVED: session creator cannot be session user");
            }
        }

        authenticator.authenticateGrant(username, password, targetUser, targetDB);

        var session = sessionRepository.persistSession(
                targetUser,
                targetDB,
                username,
                request.getReason());
        return session.getId();
    }

    private SessionManagement getSessionManagementFor(String database) {
        var config = databases.get(database);
        if (config == null) {
            // weird, shouldn't happen..
            return defaultSessionManagement;
        }

        SessionManagement sessionManagement = config.getSessionManagement();
        if (sessionManagement == null) {
            return defaultSessionManagement;
        }

        // return database-specific override
        return sessionManagement;
    }

    /**
     * Retrieve a pre-created session, or create an implicit session (when allowed).
     */
    public Session getOrCreateSession(AuthenticationRequest authenticationRequest)
            throws SessionException {

        String username = authenticationRequest.getUsername();
        String db = authenticationRequest.getDb();

        // check session
        // * when config item proxy.sessionManagement=none then just create a session
        // * otherwise (sessionManagement=required|approval) verify that one is present:
        //    * if connection-url contains session-id then fetch that and verify that username+db match
        //    * else fetch most recent session record for this username, database and verify it is found
        //    * verify that selected session is not expired
        //    * if sessionManagement=approval then verify that username != approver
        var sessionOpt = sessionRepository.getLatestSession(username, db);

        var sessionManagement = getSessionManagementFor(db);
        if (sessionManagement == SessionManagement.NONE) {
            if (sessionOpt.isEmpty() || sessionOpt.get().getExpiresAt().isBefore(Instant.now())) {
                // create new session automatically
                return sessionRepository.persistSession(
                        username,
                        db,
                        null, // approver
                        null); // reason
            }
            return sessionOpt.get();
        }

        // session is REQUIRED or APPROVED..

        if (sessionOpt.isEmpty()) {
            throw new SessionException("Session required but not present");
        }

        var session = sessionOpt.get();
        if (session.getExpiresAt().isBefore(Instant.now())) {
            throw new SessionException("Session required but not present (or expired)");
        }

        if (defaultSessionManagement == SessionManagement.APPROVED) {
            if (session.getApprover().equals(username)) {
                // this can also be checked during session creation, but there is the (small) possibility
                // that the sessionManagement config-setting is changed between session-creation and here,
                // so best to double-check.
                throw new SessionException("Session is self-approved but approval is enabled");
            }
        }

        return session;
    }
}
