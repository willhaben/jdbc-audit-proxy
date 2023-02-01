package at.willhaben.jdbcproxy.server;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Holds application configuration settings.
 */
@Component
@ConfigurationProperties(prefix="proxy")
@SuppressWarnings("unused")
public class ProxyConfig {

    // general config
    private int port = -1;
    private int maxRows = 100;
    private int maxCommandLength = 4096; // max chars in a SQL statement
    private String sessionLifetime = "PT8H"; // ISO-8601 format; see also Duration.parse
    private String socketLifetime = "PT2H"; // ISO-8601 format; see also Duration.parse
    private String queryLifetime = "PT2M"; // ISO-8601 format; see also Duration.parse

    // authentication management config
    private AuthenticationManagement authenticationManagement = AuthenticationManagement.LOCAL;
    private final LocalAuthenticator.Config localAuthConfig = new LocalAuthenticator.Config();
    private final LDAPAuthenticator.Config ldapAuthConfig = new LDAPAuthenticator.Config();

    // session management config
    private SessionManagement sessionManagement = SessionManagement.APPROVED; // make safest case the default

    // audit-database config
    private final DatasourceProvider.Config dbSessionHandlerConfig = new DatasourceProvider.Config();

    // proxied database config
    private Map<String, Database> databases;

    // ====================== general config

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getMaxRows() {
        return maxRows;
    }

    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    public int getMaxCommandLength() {
        return maxCommandLength;
    }

    public void setMaxCommandLength(int maxCommandLength) {
        this.maxCommandLength = maxCommandLength;
    }

    public String getSessionLifetime() {
        return sessionLifetime;
    }

    public void setSessionLifetime(String sessionLifetime) {
        this.sessionLifetime = sessionLifetime;
    }

    public String getSocketLifetime() {
        return socketLifetime;
    }

    public void setSocketLifetime(String socketLifetime) {
        this.socketLifetime = socketLifetime;
    }

    public String getQueryLifetime() {
        return queryLifetime;
    }

    public void setQueryLifetime(String queryLifetime) {
        this.queryLifetime = queryLifetime;
    }

    // ====================== authentication-management config

    public AuthenticationManagement getAuthenticationManagement() {
        return authenticationManagement;
    }

    public void setAuthenticationManagement(AuthenticationManagement authenticationManagement) {
        this.authenticationManagement = authenticationManagement;
    }

    public LocalAuthenticator.Config getLocalAuth() {
        return localAuthConfig;
    }

    public LDAPAuthenticator.Config getLDAPAuth() {
        return ldapAuthConfig;
    }

    // ====================== session-management config

    public SessionManagement getSessionManagement() {
        return sessionManagement;
    }

    public void setSessionManagement(SessionManagement sessionManagement) {
        this.sessionManagement = sessionManagement;
    }

    // ====================== config for audit-trail database (other stuff might also use this db for storage)

    public DatasourceProvider.Config getAuditDb() {
        return dbSessionHandlerConfig;
    }

    // ========== proxied databases config

    public Map<String, Database> getDatabases() {
        return databases;
    }

    void setDatabases(Map<String, Database> map) {
        this.databases = map;
    }

}
