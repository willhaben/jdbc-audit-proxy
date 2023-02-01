package at.willhaben.jdbcproxy.server.dao;

import at.willhaben.jdbcproxy.server.model.Session;

import java.util.Optional;

/**
 * A service which tracks "user sessions".
 * <p>
 * Sessions can be created explicitly before a user connects, or can be created "on demand" as the user logs in
 * (depending on application configuration).
 * </p>
 */
public interface SessionRepository {
    Session persistSession(String username, String db, String approver, String reason) throws SessionException;
    Optional<Session> getById(int id);
    Optional<Session> getLatestSession(String username, String db);
}
