package at.willhaben.jdbcproxy.server;

/**
 * Indicates how "user authentication" should be handled/enforced.
 * <p>
 * The application configuration file specifies which mode is active for the proxy.
 * </p>
 */
enum AuthenticationManagement {
    /**
     * There is a file on the local filesystem which holds (username, password, database) information.
     * <p>
     * When this server is clustered, this information must of course be consistent between nodes. This can
     * often be done by mounting a shared filesystem or copying a file during service setup.
     * </p>
     */
    LOCAL,

    /**
     * There is an LDAP server available which holds users and groups.
     */
    LDAP
}
