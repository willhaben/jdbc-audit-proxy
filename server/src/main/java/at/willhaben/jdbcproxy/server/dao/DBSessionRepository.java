package at.willhaben.jdbcproxy.server.dao;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Optional;

import at.willhaben.jdbcproxy.server.model.Session;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;

/**
 * Keep sessions in a database.
 */
public class DBSessionRepository implements SessionRepository {
    private final Duration sessionLifetime;
    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final RowMapper<Session> sessionRowMapper = new BeanPropertyRowMapper<>(Session.class);

    public DBSessionRepository(Duration sessionLifetime, DataSource datasource) {
        this.sessionLifetime = sessionLifetime;
        this.dataSource = datasource;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public synchronized Session persistSession(String username, String database, String approver, String reason)
            throws SessionException {

        try {
            SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate)
                    .withTableName("SESSION")
                    .usingColumns("username", "database", "at", "expiresAt", "approver", "reason")
                    .usingGeneratedKeyColumns("id");

            var at = Instant.now();
            var expiresAt = at.plus(sessionLifetime);

            var cols = new HashMap<String, Object>();
            cols.put("username", username);
            cols.put("database", database);
            cols.put("at", new java.sql.Timestamp(at.toEpochMilli()));
            cols.put("expiresAt", new java.sql.Timestamp(expiresAt.toEpochMilli()));
            cols.put("approver", approver);
            cols.put("reason", reason);
            int id = simpleJdbcInsert.executeAndReturnKey(cols).intValue();

            var session = new Session();
            session.setId(id);
            session.setUsername(username);
            session.setDatabase(database);
            session.setAt(at);
            session.setExpiresAt(expiresAt);
            session.setApprover(approver);
            session.setReason(reason);
            return session;
        } catch(RuntimeException e) {
            throw new SessionException("Internal error: unable to create session", e);
        }
    }

    @Override
    public synchronized Optional<Session> getById(int id) {
        try {
            var session = jdbcTemplate.queryForObject(
                    "select * from SESSION where id = ?",
                    sessionRowMapper,
                    id);
            return Optional.of(session);
        } catch(EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public synchronized Optional<Session> getLatestSession(String username, String db) {
        try {
            var session = jdbcTemplate.queryForObject(
                    "select * from SESSION where username = ? and database = ? order by expiresAt desc limit 1",
                    sessionRowMapper,
                    username, db);
            return Optional.of(session);
        } catch(EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }
}
