package at.willhaben.jdbcproxy.driver;

public class ProxyError extends Exception {
    public ProxyError(String message) {
        super(message);
    }

    public ProxyError(String message, Throwable cause) {
        super(message, cause);
    }
}
