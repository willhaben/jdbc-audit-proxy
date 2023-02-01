package at.willhaben.jdbcproxy.driver;

import at.willhaben.jdbcproxy.server.avro.ErrorResponse;
import at.willhaben.jdbcproxy.server.avro.Request;
import at.willhaben.jdbcproxy.server.avro.Response;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Handle the low-level details of exchanging data across the network with the remote jdbcauditproxy server.
 */
public class Communicator {
    private static final String EXCEPTION_SOCKET_CLOSED = "Failed to read data from proxyserver: socket closed";
    private static final String EXCEPTION_UNKNOWN = "Failed to read data from proxyserver: cause unknown";

    private final Socket socket;

    public Communicator(Socket socket) {
        this.socket = socket;
    }

    synchronized
    <T extends SpecificRecord> T send(SpecificRecord out, Class<T> expectedResponse) throws ProxyError, ConnectionClosedError {
        if (isClosed()) {
            throw new ConnectionClosedError();
        }

        try {
            return doSend(out, expectedResponse);
        } catch(ProxyError e) {
            // Sadly there is no generally reliable system for logging messages here...
            System.err.println(String.format(
                    "failed request of type %s -> %s: %s",
                    out.getClass().getName(), expectedResponse.getName(), e.getMessage()));
            try {
                this.close();
            } catch(IOException e2) {
                // ignore - we are already throwing an exception
            }
            throw e;
        }
    }

    private <T extends SpecificRecord> T doSend(SpecificRecord out, Class<T> expectedResponse) throws ProxyError {
        Request request = Request.newBuilder().setRequest(out).build();
        try {
            serializeAvroRequest(request, socket.getOutputStream());
            var response = deserializeAvroResponse(Response.class, socket).getResponse();
            if (response instanceof ErrorResponse) {
                ErrorResponse errorResponse = (ErrorResponse) response;
                throw new ProxyError(errorResponse.getMessage());
            }
            return expectedResponse.cast(response);
        } catch(IOException e) {
            throw new ProxyError("Unknown error: " + e.getMessage(), e);
        }
    }

    void close() throws IOException {
        if (socket != null) {
            socket.close();
        }
    }

    boolean isClosed() {
        return socket == null || socket.isClosed();
    }

    void serializeAvroRequest(SpecificRecord request, OutputStream target) throws ProxyError {
        DatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(request.getSchema());
        Encoder encoder = EncoderFactory.get().binaryEncoder(target, null);
        try {
            writer.write(request, encoder);
            encoder.flush();
            target.flush();
        } catch (IOException e) {
            throw new ProxyError("Serialization error:" + e.getMessage());
        }
    }

    <T extends SpecificRecord> T deserializeAvroResponse(Class<T> clazz, Socket socket) throws ProxyError {
        DatumReader<T> reader = new SpecificDatumReader<>(clazz);
        try {
            if (socket.isClosed()) {
                throw new ProxyError(EXCEPTION_SOCKET_CLOSED);
            }

            var inputStream = socket.getInputStream();
            if (inputStream == null) {
                throw new ProxyError(EXCEPTION_SOCKET_CLOSED);
            }

            // Read the object. Note that a max read timeout is set in class ProxyDriver when the
            // socket is created..
            Decoder decoder = DecoderFactory.get().directBinaryDecoder(socket.getInputStream(), null);
            return reader.read(null, decoder);
        } catch (IOException e) {
            var msg = e.getMessage();
            if (msg != null) {
                throw new ProxyError("Failed to read data from proxyserver: " + msg);
            } else if (socket.isClosed()) {
                throw new ProxyError(EXCEPTION_SOCKET_CLOSED);
            } else {
                throw new ProxyError(EXCEPTION_UNKNOWN);
            }
        }
    }
}
