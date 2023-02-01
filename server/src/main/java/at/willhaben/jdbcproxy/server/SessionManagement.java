package at.willhaben.jdbcproxy.server;

/**
 * Indicates how "user sessions" should be handled/enforced.
 * <p>
 * A session represents "a period of time in which a user is authorized to connect". Depending on settings, such
 * authorization may be recorded "on the fly" or may need to be pre-inserted into the database before a user
 * tries to connect to a specific database. A session is valid for any number of connections within the
 * specified time-period.
 * </p>
 * <p>
 * The application configuration file specifies which mode is active for the proxy.
 * </p>
 */
enum SessionManagement {
    /**
     * Sessions can be created on-demand as users connect. This means that there is no "reason" information
     * available as to why this user is connecting. There is also no "four-eyes access control".
     */
    NONE,

    /**
     * Sessions must be created before a user connects via JSBC - including registering a "reason" for the
     * access. However a user can create session records for themselves, ie no "four-eyes control".
     */
    REQUIRED,

    /**
     * Sessions for user X must be created/defined by some other user before X can connect - ie
     * "four eyes access control" is applied.
     */
    APPROVED
}
