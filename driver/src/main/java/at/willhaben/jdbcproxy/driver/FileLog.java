package at.willhaben.jdbcproxy.driver;

import java.io.*;

/**
 * Hacky minimal logging-library implementation.
 * <p>
 * Because this is a "plugin" for environments over which the user has little control, it isn't appropriate
 * to add SLF4J logging or similar. Even java.util.logging could be problematic. So this trivial implementation
 * allows an environment-variable to specify an output-file for logging from this driver. When the env-var is
 * not defined, logging is disabled.
 * </p>
 */
public class FileLog {
    public static final String LOGGING_ENV_VAR_NAME = "JDBCPROXY_LOGFILE";
    private static PrintWriter writer = openFile();

    private static PrintWriter openFile() {
        String logfilename = System.getenv(LOGGING_ENV_VAR_NAME);
        if (logfilename == null) {
            return null;
        }

        try {
            OutputStream os = new FileOutputStream(logfilename);
            return new PrintWriter(new OutputStreamWriter(os), true);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isEnabled() {
        return (writer != null);
    }

    public static void log(String msg) {
        if (writer == null) {
            return;
        }

        try {
            writer.println(msg);
        } catch(Exception e) {
            // ignore
        }
    }

    public static void log(Exception cause) {
        if (writer == null) {
            return;
        }

        cause.printStackTrace(writer);
    }

    public static void log(String msg, Exception cause) {
        if (writer == null) {
            return;
        }

        try {
            writer.println(msg);
            cause.printStackTrace(writer);
        } catch(Exception e) {
            // ignore
        }
    }
}
