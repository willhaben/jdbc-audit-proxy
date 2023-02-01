package at.willhaben.jdbcproxy.server;

import at.willhaben.jdbcproxy.server.avro.*;
import at.willhaben.jdbcproxy.server.dao.AuditLogger;
import at.willhaben.jdbcproxy.server.dao.SessionException;
import at.willhaben.jdbcproxy.server.model.Login;
import at.willhaben.jdbcproxy.server.model.Session;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Handle a connected client.
 * <p>
 * When request is a SessionRequest then create the session and terminate the connection.
 * </p>
 * <p>
 * Otherwise require an AuthorizationRequest and validate it; when not valid then terminate the connection.
 * Then repeatedly read a request-packet from the socket and pass it to the RequestHandler for processing.
 * </p>
 */
class ConnectionHandler implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionHandler.class);

    private final int maxRows;
    private final List<String> schemas;
    private final SessionManager sessionManager;
    private final Socket socket;
    private final Duration maxSocketLifetime;
    private final Map<String, Database> databases;
    private final Map<String, DataSource> dataSources;
    private final Watchdog watchdog;
    private final AuditLogger auditLogger;
    private final Authenticator authenticator;

    private String username;
    private Instant connectedAt;
    private String db;

    ConnectionHandler(
            int maxRows,
            List<String> schemas,
            SessionManager sessionManager,
            Socket socket,
            Duration maxSocketLifetime,
            Map<String, Database> databases,
            Map<String, DataSource> dataSources,
            Watchdog watchdog,
            AuditLogger auditLogger,
            Authenticator authenticator) {
        this.maxRows = maxRows;
        this.schemas = schemas;
        this.sessionManager = sessionManager;
        this.socket = socket;
        this.maxSocketLifetime = maxSocketLifetime;
        this.databases = databases;
        this.dataSources = dataSources;
        this.watchdog = watchdog;
        this.auditLogger = auditLogger;
        this.authenticator = authenticator;
    }

    @Override
    public void run() {
        try (var watch = watchdog.closeAfter(socket, maxSocketLifetime)) {
            var request = receive();

            if (request instanceof SessionRequest sr) {
                // This message-type triggers a single request/response exchange followed by
                // closing the socket.
                int sessionId = sessionManager.createSession(sr);
                send(SessionResponse.newBuilder().setSessionId(sessionId).build());
                logger.info("Created new session by {} for {}", sr.getUsername(), sr.getForUsername());
                return;
            }

            if (request instanceof AuthenticationRequest == false) {
                throw new IOException("Invalid first packet - expected connection request");
            }

            // Validate authentication data, then store username for later logging
            Session session;
            var authenticationRequest = (AuthenticationRequest) request;
            try {
                session = validate(authenticationRequest);
            } catch (IOException e) {
                logger.warn("Session validation failed for user {}", authenticationRequest.getUsername());
                send(AuthenticationResponse.newBuilder()
                        .setAccepted(false)
                        .setErrorMessage(e.getMessage())
                        .build());
                return;
            }

            auditLogger.recordLogin(session);
            watch.setUsername(session.getUsername());

            this.username = authenticationRequest.getUsername();
            this.connectedAt = Instant.now();
            this.db = authenticationRequest.getDb();

            var dataSource = dataSources.get(this.db);
            if (dataSource == null) {
                logger.warn("Unknown target database: user={} db={}", username, db);
                send(AuthenticationResponse.newBuilder()
                        .setAccepted(false)
                        .setErrorMessage("Unknown database: " + db)
                        .build());
                return;
            }

            var login = new Login(session);

            send(AuthenticationResponse.newBuilder().setAccepted(true).build());

            logger.info("Session validation succeeded for user={} db={}", username, db);

            RequestHandler requestHandler = new RequestHandler(
                    login,
                    schemas,
                    maxRows,
                    dataSource,
                    watchdog,
                    auditLogger);

            // Now enter a loop, reading requests and passing them to the requestHandler for processing.
            boolean done = false;
            while (!done) {
                request = receive();
                if (request == null) {
                    logger.info("Client closed socket (EOF): user={}", username);
                    done = true;
                    continue;
                }

                // watchdog.reschedule(watchId, now + 1hour)
                if (request instanceof CloseRequest) {
                    done = true;
                    send(new CloseResponse());
                    logger.info("Connection closed: user={} database={}", username, db);
                    continue;
                }

                var response = requestHandler.handleRequest(request);
                if (response == null) {
                    response = ErrorResponse.newBuilder().setMessage("Unsupported request type").build();
                }
                send(response);
            }
        } catch (SessionException e) {
            logger.warn("Failure while authenticating remote user!", e);
            try {
                var em = e.getMessage();
                var msg = (em == null) ? "Authentication failure" : "Authentication failure: " + em;
                send(ErrorResponse.newBuilder().setMessage(msg).build());
            } catch (IOException e2) {
                logger.warn("Unable to tell client about the error!");
            }
        } catch (IOException | SQLException | RuntimeException e) {
            logger.warn("Failure while communicating to underlying database or to client. Closing Connection! ", e);
            try {
                send(ErrorResponse.newBuilder().setMessage("Unknown error").build());
            } catch (IOException e2) {
                logger.warn("Unable to tell client about the error!");
            }
        } finally {
            try {
                socket.close();
            } catch (IOException e2) {
                //ignore
            }
            logger.info("Connection terminated: user={} database={} connectedAt={}", username, db, connectedAt);
        }
    }

    public void stop() {
        try {
            socket.close();
        } catch (IOException e) {
            // ignore
        }
    }

    void send(SpecificRecord response) throws IOException {
        var rsp = Response.newBuilder().setResponse(response).build();
        serializeAvroResponse(rsp, socket.getOutputStream());
    }

    Object receive() throws IOException {
        try {
            var request = deserializeAvroRequest(Request.class, socket.getInputStream());
            if (request == null) {
                // eof
                logger.info("No AVRO request read; assuming EOF");
                return null;
            }
            return request.getRequest();
        } catch(RuntimeException e) {
            // avro sometimes throws RuntimeExceptions, eg ArrayIndexOutOfBoundsException
            throw new IOException("Unable to parse message from client", e);
        }
    }

    void serializeAvroResponse(SpecificRecord request, OutputStream target) {
        DatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(request.getSchema());
        Encoder encoder = EncoderFactory.get().binaryEncoder(target, null);
        try {
            writer.write(request, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new RuntimeException("Serialization error:" + e.getMessage());
        }
    }

    <T extends SpecificRecord> T deserializeAvroRequest(Class<T> clazz, InputStream in) {
        DatumReader<T> reader = new SpecificDatumReader<>(clazz);
        Decoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
        try {
            return reader.read(null, decoder);
        } catch (EOFException | SocketException e) {
            // socket closed
            logger.info("Failed to read avro message", e);
            return null;
        } catch (IOException e) {
            throw new RuntimeException("Deserialization error:" + e.getClass() + ":" + e.getMessage());
        }
    }

    /**
     * Ensure that the specified user is allowed to access the specified database.
     * <p>
     * User credentials must be correct, user must have appropriate group-settings to access the desired
     * database, and there must be a valid user-session (pre-existing or created-on-demand when allowed).
     * </p>
     */
    Session validate(AuthenticationRequest authenticationRequest) throws IOException, SessionException {
        authenticator.authenticateAccess(
                authenticationRequest.getUsername(),
                authenticationRequest.getPassword(),
                authenticationRequest.getDb());

        return sessionManager.getOrCreateSession(authenticationRequest);
    }
}
