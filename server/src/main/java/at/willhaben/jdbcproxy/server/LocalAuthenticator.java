package at.willhaben.jdbcproxy.server;

import at.willhaben.jdbcproxy.server.dao.SessionException;

import javax.security.sasl.AuthenticationException;
import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implement authentication of users by reading data from a local file.
 */
public class LocalAuthenticator implements Authenticator {
    public static class Config {
        private String filename;
        public String getFilename() {
            return filename;
        }
        public void setFilename(String filename) {
            this.filename = filename;
        }
    }

    public static class User {
        final String credential;
        final List<String> databases;

        User(String credential, List<String> databases) {
            this.credential = credential;
            this.databases = databases;
        }
    }

    private interface Validator {
        void validate(User user) throws SessionException, IOException;
    }

    // ============================================================================================

    private final Map<String, User> users;

    public LocalAuthenticator(Config config) throws IOException {
        var filename = config.getFilename();
        if (filename == null) {
            throw new IOException("Local authenticator configured, but no filename specified");
        }

        users = loadUsers(new FileReader(filename));
    }

    static Map<String, User> loadUsers(Reader source) throws IOException {
        Map<String, User> users = new HashMap<>();
        try (var inputStream = new BufferedReader(source)) {
            for(;;) {
                var line = inputStream.readLine();
                if (line == null) {
                    break;
                }

                var items = line.split(":");
                if (items.length != 3) {
                    throw new IOException("Invalid line: " + line);
                }

                var username = items[0];
                var cred = items[1];
                var databases = List.of(items[2].split(","));

                users.put(username, new User(cred, databases));
            }
        }

        return users;
    }

    public void authenticateAccess(String username, String password, String database) throws SessionException, IOException {
        // Verify that the connecting user is known and credentials are valid.
        // Then also check that they have the specified database in their "allowed db list"
        authenticate(
            username,
            password,
            user -> {
                if (!user.databases.contains(database)) {
                    throw new SessionException("Database not permitted");
                }
            });
    }

    @Override
    public void authenticateGrant(String username, String password, String targetUser, String targetDatabase) throws SessionException, IOException {
        // For session-grants, it is sufficient to verify that the "granting" user is known and credentials are valid.
        // The "granted" user must still have rights to access the granted DB (checked on connect).
        authenticate(
            username,
            password,
            user -> {});
    }

    private void authenticate(String username, String password, Validator validator) throws SessionException, IOException {
        var user = users.get(username);

        if (user == null) {
            throw new SessionException("User unknown");
        }

        // For now, just use plain text comparison. This could be replaced by hash(password) if desired,
        // or even some more sophisticated scheme that doesn't require the caller to provide their password
        // in plain text.
        if (!user.credential.equals(password)) {
            throw new SessionException("Credential Invalid");
        }

        validator.validate(user);
    }
}
