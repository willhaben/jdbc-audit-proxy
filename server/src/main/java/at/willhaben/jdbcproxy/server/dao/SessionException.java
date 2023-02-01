package at.willhaben.jdbcproxy.server.dao;

/**
 * Represents an error accessing user-session information.
 */
public class SessionException extends Exception {
    public SessionException(String reason) {
        super(reason);
    }

    public SessionException(String reason, Throwable cause) {
        super(reason, cause);
    }
}
