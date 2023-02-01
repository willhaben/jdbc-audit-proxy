package at.willhaben.jdbcproxy.driver;

import at.willhaben.jdbcproxy.server.avro.SessionRequest;
import at.willhaben.jdbcproxy.server.avro.SessionResponse;

import java.io.IOException;
import java.net.Socket;
import java.util.*;

/**
 * Commandline entry-point for communicating with a jdbc-audit-proxy-server when not in the context of a JDBC
 * connection.
 * <p>
 * One example is the ability to send a "create session" request to the server. Depending on how the server
 * is configured, a session might need to be created for a user before a JDBC connection can be made. Sessions
 * can be used to ensure a "reason" is associated with each JDBC connection, or to enforce "4 eyes" access,
 * ie where access for a user must be "pre-approved" by a second user.
 * </p>
 */
public class ProxyClient {
    public static void main(String args[]) {
        if (args.length == 0) {
            printUsage();
            System.exit(-1);
        }

        Deque<String> items = new ArrayDeque<>(Arrays.asList(args));

        var command = items.pop();
        if (command.startsWith("--")) {
            System.err.println("Command missing. Valid commands: ['create-session']");
            printUsage();
            System.exit(-1);
        }

        Map<String, String> opts = parseArgs(items);
        if ("create-session".equals(command)) {
            doCreateSession(opts);
        } else {
            printUsage();
            System.exit(-1);
        }
    }

    private static void printUsage() {
        System.err.println(
                "Usage:\n" +
                        "echo 'password:' && read -s password # store password in $password\n" +
                        "java -jar driver.jar\n" +
                        "  create-session --proxyserver host:port --approver {user} --password $password\n" +
                        "    --database {database} --reason {reason} [--targetUser {user}]");
    }

    private static Map<String, String> parseArgs(Deque<String> in) {
        Map<String, String> opts = new HashMap<>();
        while (!in.isEmpty()) {
            var opt = in.pop();
            if (!opt.startsWith("--")) {
                throw new IllegalArgumentException(String.format(
                        "Invalid args: expected --{someoption} but got '%s'",
                        opt));
            }

            if (in.isEmpty()) {
                throw new IllegalArgumentException("Invalid args: expected value");
            }
            var val = in.pop();

            opts.put(opt.substring(2), val);
        }
        return opts;
    }

    private static String getMandatory(Map<String, String> opts, String key) {
        var value = opts.remove(key);
        if (value == null || value.length() == 0) {
            throw new IllegalArgumentException("Missing mandatory option: " + key);
        }
        return value;
    }

    @SuppressWarnings("squid:S2093") // suppress sonar: socket lifetime not limited to this method
    private static void doCreateSession(Map<String, String> opts) {
        var target = getMandatory(opts, "proxyserver");
        var db = getMandatory(opts, "database");
        var reason = getMandatory(opts, "reason");
        var approver = getMandatory(opts, "approver");
        var password = getMandatory(opts, "password");
        var targetUser = (opts.containsKey("targetUser") ? opts.remove("targetUser") : approver);

        var targetParts = target.split(":");
        var host = targetParts[0];
        var port = Integer.parseInt(targetParts[1]);

        if (!opts.isEmpty()) {
            System.err.println("Unexpected options present:" + String.join(",", opts.keySet()));
            return;
        }

        Socket socket = null;
        try {
            socket = new Socket(host, port);
            Communicator communicator = new Communicator(socket);
            var request = SessionRequest.newBuilder()
                    .setDb(db)
                    .setReason(reason)
                    .setUsername(approver)
                    .setPassword(password)
                    .setForUsername(targetUser)
                    .build();
            var response = communicator.send(request, SessionResponse.class);
            System.out.println("Session created: " + response.getSessionId());
        } catch(IOException e) {
            System.err.println("Failed to communicate to server");
        } catch(ProxyError e) {
            System.err.println("Server declined request: " + e.getMessage());
        } catch(ConnectionClosedError e) {
            System.err.println("Connection terminated by server");
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch(Exception e) {
                    // ignore
                }
            }
        }
    }
}
