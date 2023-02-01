package at.willhaben.jdbcproxy.server.dao;

import at.willhaben.jdbcproxy.server.model.Session;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Keep sessions in memory.
 * <p>
 * When using this SessionHandler and "pre-authorized sessions", the jdbc-audit-proxy obviously cannot be behind a
 * load-balancer, as new user-connections need to arrive at the appropriate instance which already has the relevant
 * Session object in memory.
 * </p>
 */
public class MemSessionRepository implements SessionRepository {
    private final Duration sessionLifetime;

    private int nextId;
    private final Map<Integer, Session> sessionsById = new HashMap<>();
    private final Map<String, Session> sessionsByNameAndDB = new HashMap<>();

    public MemSessionRepository(Duration sessionLifetime) {
        this.sessionLifetime = sessionLifetime;
    }

    @Override
    public synchronized Session persistSession(String username, String database, String approver, String reason) {
        nextId = nextId + 1;

        var at = Instant.now();
        var expiresAt = at.plus(sessionLifetime);

        var session = new Session();
        session.setId(nextId);
        session.setUsername(username);
        session.setDatabase(database);
        session.setAt(at);
        session.setExpiresAt(expiresAt);
        session.setApprover(approver);
        session.setReason(reason);

        sessionsById.put(session.getId(), session);
        sessionsByNameAndDB.put(makeKey(username, database), session);
        return session;
    }

    @Override
    public Optional<Session> getById(int id) {
        return Optional.ofNullable(sessionsById.get(id));
    }

    @Override
    public synchronized Optional<Session> getLatestSession(String username, String db) {
        return Optional.ofNullable(sessionsByNameAndDB.get(makeKey(username, db)));
    }

    private String makeKey(String username, String db) {
        return String.format("%s/%s", username, db);
    }
}
