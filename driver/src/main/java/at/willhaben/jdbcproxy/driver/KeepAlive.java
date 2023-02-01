package at.willhaben.jdbcproxy.driver;

import at.willhaben.jdbcproxy.server.avro.PingRequest;
import at.willhaben.jdbcproxy.server.avro.PingResponse;

import java.time.Duration;

/**
 * A simple "background task" which sends a "PingRequest" message to the server every N seconds in order
 * to ensure that the socket between client and server is not auto-closed due to inactivity.
 * <p>
 * Ideally this feature could be configured server-side, either to set the interval or to disable this
 * completely if not needed. However the current client/server protocol doesn't have the ability to
 * return such "connection settings" - ie the AVRO schema would need updating. Perhaps AuthenticationResponse
 * could be extended with such settings, or new "ConfigurationRequest/ConfigurationResponse" messages could
 * be defined.
 * </p>
 * <p>
 * This implementation could be improved to skip keepalives if other messages have been sent via the Communicator.
 * However that is only a small optimisation and possibly not worth it.
 * </p>
 */
public class KeepAlive implements Runnable {
    private static final Duration pollInterval = Duration.ofSeconds(10);
    private final Communicator communicator;
    private final PingRequest pingRequest;

    public KeepAlive(Communicator communicator) {
        this.communicator = communicator;
        this.pingRequest = new PingRequest();
    }

    /**
     * Send a PingRequest every pollInterval, returning (ie terminating enclosing thread) when the socket
     * to the server is closed.
     */
    @Override
    public void run() {
        for(;;) {
            try {
                Thread.sleep(pollInterval.toMillis());
                communicator.send(pingRequest, PingResponse.class);
            } catch(InterruptedException e) {
                // ignore
            } catch(ConnectionClosedError e) {
                break;
            } catch(Exception e) {
                // log and terminate - at worst the keepalive will stop working.
                System.err.println("KeepAlive received exception:" + e.getMessage());
                e.printStackTrace(System.err);
                break;
            }
        }
    }
}
