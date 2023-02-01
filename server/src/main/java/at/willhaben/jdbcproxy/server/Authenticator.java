package at.willhaben.jdbcproxy.server;

import at.willhaben.jdbcproxy.server.dao.SessionException;

import java.io.IOException;

public interface Authenticator {
    /**
     * Verify that the specified user has the specified credentials, and has rights to access
     * the specified database.
     * <p>
     * On success, returns. On failure, throws exception.
     * </p>
     */
    void authenticateAccess(String username, String password, String database) throws SessionException, IOException;

    /**
     * Verify that the specified user has the specified credentials and has the rights
     * to grant the target user access to the target database.
     * <p>
     * Used in "four-eyes authorization", where user A must "approve" access for user B
     * before B can access a specific database.
     * </p>
     */
    void authenticateGrant(String username, String password, String targetUser, String targetDatabase) throws SessionException, IOException;
}
