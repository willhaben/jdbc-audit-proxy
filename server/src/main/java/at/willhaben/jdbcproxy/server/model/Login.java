package at.willhaben.jdbcproxy.server.model;

import java.time.Instant;

public class Login {
    private final Session session;
    private final Instant at;

    public Login(Session session) {
        this.session = session;
        this.at = Instant.now();
    }

    public Session getSession() {
        return session;
    }

    public Instant getAt() {
        return at;
    }
}
