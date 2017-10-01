package ru.mail.polis.malcev;

import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public class MyService implements KVService {

    @NotNull
    private final HttpServer server;

    @NotNull
    private final MyDAO dao;

    private static final String PREFIX = "id=";

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

            if (id.equals("")) {
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
                    final int contentLength = Integer.valueOf(http.getRequestHeaders().getFirst("Content-Length"));
                    final byte[] putValue = new byte[contentLength];

                    int read = http.getRequestBody().read(putValue);

                    dao.upsert(id,putValue);
                    http.sendResponseHeaders(201, 0);
                    break;
                default:
                    http.sendResponseHeaders(405,0);
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
