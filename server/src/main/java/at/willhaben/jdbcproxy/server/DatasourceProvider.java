package at.willhaben.jdbcproxy.server;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

/**
 * A simple utility that determines whether the application configuration has specified a database to use
 * for session-tracking and audit-log-storage, and if so then return a suitable Datasource.
 */
public final class DatasourceProvider {
    /**
     * POJO populated from application configuration data.
     * <p>
     * Currently used only for the audit-db configuration settings; the set of target databases is
     * configured via POJO class "Database".
     * </p>
     */
    public static class Config {
        private String driverClass; // name of the JDBC driver class to load
        private String jdbcUrl; // URL to pass to the JDBC driver
        private String username;
        private String password;

        public String getDriverClass() {
            return driverClass;
        }

        public void setDriverClass(String driverClass) {
            this.driverClass = driverClass;
        }

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public void setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
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
    }

    public static DataSource createDataSource(Config config) {
        if (config.getJdbcUrl() == null) {
            return null;
        }

        // Allow the JDBC driver to register itself with the JDBC DriverManager infrastructure by
        // triggering class-resolution.
        if (config.getDriverClass() != null) {
            try {
                Class.forName(config.getDriverClass());
            } catch(ClassNotFoundException e) {
                throw new RuntimeException(
                        String.format("Unable to load specified driver class '%s'", config.getDriverClass()));
            }
        }

        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(config.getJdbcUrl());
        hc.setUsername(config.getUsername());
        hc.setPassword(config.getPassword());
        hc.addDataSourceProperty("cachePrepStmts" , "true");
        hc.addDataSourceProperty("prepStmtCacheSize" , "250");
        hc.addDataSourceProperty("prepStmtCacheSqlLimit" , "2048");
        return new HikariDataSource(hc);
    }

    public static DataSource createDataSource(Database config) {
        HikariConfig hc = new HikariConfig();
        hc.setDriverClassName(config.getDriverClass());
        hc.setJdbcUrl(config.getUrl());
        hc.setUsername(config.getUsername());
        hc.setPassword(config.getPassword());
        hc.setConnectionTimeout(10000);
        hc.setMinimumIdle(0);

        // attempt to make connections read-only
        //
        // Note however that the javadoc says this:
        // > Puts this connection in read-only mode as a hint to the driver
        // > to enable database optimizations
        // ie this is not a guarantee. It is safer to configure the DB connection with
        // credentials of a user with select-only rights.
        //
        // Also, Postgres ignores this when autoCommit=true
        hc.setReadOnly(true);
        hc.setAutoCommit(false);

        hc.setInitializationFailTimeout(-1); // don't create connection now..alternative is to catch and ignore exception
        return new HikariDataSource(hc);
    }

    private DatasourceProvider() {
        // this is a static utility class
    }
}
