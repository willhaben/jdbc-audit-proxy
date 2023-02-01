package at.willhaben.jdbcproxy.server;

/**
 * Configuration settings for a target database that a user can connect to.
 */
public class Database {
    private String driverClass;
    private String url;
    private String username;
    private String password;
    private boolean audited = true; // whether to keep an audit-trail or not
    private SessionManagement sessionManagement = null; // null means "use global default"

    void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getDriverClass() {
        return driverClass;
    }

    void setUrl(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isAudited() {
        return audited;
    }

    public void setAudited(boolean state) {
        audited = state;
    }

    public SessionManagement getSessionManagement() {
        return sessionManagement;
    }

    /**
     * Optionally, override the global default for this particular database. When
     * not defined (null), the global default will be used.
     */
    public void setSessionManagement(SessionManagement sessionManagement) {
        this.sessionManagement = sessionManagement;
    }
}
