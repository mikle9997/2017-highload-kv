package ru.mail.polis.malcev;

import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public class MyService implements KVService {

    @NotNull
    private final HttpServer server;

    @NotNull
    private final MyDAO dao;

    private static final String PREFIX = "id=";

    private static final int SIZE_OF_BUFFER = 1024;

    public MyService(int port, MyDAO dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port),0);
        this.dao = dao;

        this.server.createContext("/v0/status", http -> {
            final String response = "ONLINE";
            http.sendResponseHeaders(200, response.length());
            http.getResponseBody().write(response.getBytes());
            http.close();
        });

        this.server.createContext("/v0/entity", http -> {
            final String id = extractId(http.getRequestURI().getQuery());

            if ("".equals(id)) {
                http.sendResponseHeaders(400,0);
                http.close();
                return;
            }

            switch (http.getRequestMethod()){
                case "GET" :
                    try {
                        final byte[] getValue = dao.get(id);
                        http.sendResponseHeaders(200, getValue.length);
                        http.getResponseBody().write(getValue);
                    } catch (NoSuchElementException | IOException e) {
                        http.sendResponseHeaders(404,0);
                    }
                    break;

                case "DELETE" :
                    dao.delete(id);
                    http.sendResponseHeaders(202, 0);
                    break;

                case "PUT" :
                    InputStream requestStream = http.getRequestBody();
                    byte[] buffer = new byte[SIZE_OF_BUFFER];
                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()){
                        while (requestStream.read(buffer) != -1) {
                            baos.write(buffer);
                        }
                        dao.upsert(id,baos.toByteArray());
                    }
                    http.sendResponseHeaders(201, 0);
                    break;
                default:
                    http.sendResponseHeaders(405,0);
                    break;
            }
            http.close();
        });
    }

    @NotNull
    private static String extractId(@NotNull final String query) {
        if(!query.startsWith(PREFIX)) {
            throw new IllegalArgumentException("Wrong string");
        }
        return query.substring(PREFIX.length());
    }

    @Override
    public void start() {
        this.server.start();
    }

    @Override
    public void stop() {
        this.server.stop(0);
    }
}
