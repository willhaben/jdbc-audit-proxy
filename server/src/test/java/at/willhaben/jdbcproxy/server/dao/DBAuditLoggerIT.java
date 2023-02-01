package at.willhaben.jdbcproxy.server.dao;

import at.willhaben.jdbcproxy.server.BaseIT;
import at.willhaben.jdbcproxy.server.DatasourceProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Map;

public class DBAuditLoggerIT extends BaseIT {
    private DataSource datasource;

    @Before
    public void setup() {
        var config = new DatasourceProvider.Config();
        config.setJdbcUrl("jdbc:tc:postgresql:13.2:////postgres?TC_INITSCRIPT=file:dbsetup.sql");
        config.setUsername("postgres");
        config.setPassword("postgres");
        datasource = DatasourceProvider.createDataSource(config);
    }

    @Test
    public void testPersistLogin() throws Exception {
        DBSessionRepository handler = new DBSessionRepository(Duration.ofHours(1), datasource);
        var session = handler.persistSession("somebody", "somedb", null, null);
        Assert.assertNotEquals(0, session.getId());

        DBAuditLogger auditLogger = new DBAuditLogger(Map.of(), datasource, 1000);

        var login = auditLogger.recordLogin(session);
        auditLogger.recordOperation(login, "operation1");
        auditLogger.recordOperation(login, "operation2");

        // verify that the database has indeed been updated
        var jdbcTemplate = new JdbcTemplate(datasource);
        var commands = jdbcTemplate.queryForList(
                "select command from OPERATION where session = ?",
                String.class,
                session.getId());
        Assert.assertEquals(2, commands.size());
    }
}
