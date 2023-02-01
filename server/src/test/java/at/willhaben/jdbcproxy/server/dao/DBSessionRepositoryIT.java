package at.willhaben.jdbcproxy.server.dao;

import at.willhaben.jdbcproxy.server.BaseIT;
import at.willhaben.jdbcproxy.server.DatasourceProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.time.Duration;

public class DBSessionRepositoryIT extends BaseIT {
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
    public void testPersistSession() throws Exception {
        DBSessionRepository handler = new DBSessionRepository(Duration.ofHours(1), datasource);

        // verify load by id with missing entity
        var noSession = handler.getById(117);
        Assert.assertTrue(noSession.isEmpty());

        // verify load by name/db with missing entity
        var noLatestSession = handler.getLatestSession("somebody", "somedb");
        Assert.assertTrue(noLatestSession.isEmpty());

        // verify persistence with null approver/reason
        var newSession1 = handler.persistSession("somebody", "somedb", null, null);
        Assert.assertNotEquals(0, newSession1.getId());
        Assert.assertEquals("somebody", newSession1.getUsername());
        Assert.assertEquals("somedb", newSession1.getDatabase());
        Assert.assertNull(newSession1.getApprover());
        Assert.assertNull(newSession1.getReason());
        Assert.assertNotNull(newSession1.getAt()); // auto-initialized
        Assert.assertNotNull(newSession1.getExpiresAt()); // auto-initialized

        // verify load by id with matching entity
        var session = handler.getById(newSession1.getId());
        Assert.assertTrue(session.isPresent());
        Assert.assertEquals(newSession1.getId(), session.get().getId());
        Assert.assertEquals(newSession1.getUsername(), session.get().getUsername());

        // verify load by name/db with 1 matching entity
        var latestSession = handler.getLatestSession("somebody", "somedb");
        Assert.assertTrue(latestSession.isPresent());
        Assert.assertEquals("somebody", newSession1.getUsername());
        Assert.assertEquals("somedb", newSession1.getDatabase());
        Assert.assertNull(newSession1.getApprover());
        Assert.assertNull(newSession1.getReason());
        Assert.assertNotNull(newSession1.getAt());
        Assert.assertNotNull(newSession1.getExpiresAt());

        // verify persistence with non-null approver/reason
        var newSession2 = handler.persistSession("somebody", "somedb", "otherbody", "just because");
        Assert.assertNotEquals(0, newSession2.getId());
        Assert.assertEquals("somebody", newSession2.getUsername());
        Assert.assertEquals("somedb", newSession2.getDatabase());
        Assert.assertEquals("otherbody", newSession2.getApprover());
        Assert.assertEquals("just because", newSession2.getReason());
        Assert.assertNotNull(newSession2.getAt()); // auto-initialized
        Assert.assertNotNull(newSession2.getExpiresAt()); // auto-initialized
        Assert.assertTrue(newSession2.getAt().isAfter(newSession1.getAt()));

        // verify load by name/db with multiple matching entities (latest returned)
        var latestSession2 = handler.getLatestSession("somebody", "somedb");
        Assert.assertTrue(latestSession2.isPresent());
        Assert.assertEquals("somebody", newSession2.getUsername());
        Assert.assertEquals("somedb", newSession2.getDatabase());
    }
}
