package ru.mail.polis.malcev;

import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;


public class MyService implements KVService {

    @NotNull
    private final HttpServer server;

    @NotNull
    private final MyDAO dao;

    private final Set<String> topology;

    private static final String ID_PREFIX = "id=";

    private static final String REPLICAS_PREFIX = "&replicas=";

    private static final String SLASH = "/";

    private static final int SIZE_OF_BUFFER = 1024;

    private static class  Replicas {

        private int ack = 0;

        private int from = 0;

        private boolean exist = true;

        public Replicas(boolean exist) {
            this.exist = exist;
        }

        public Replicas(int ack, int from) {
            this.ack = ack;
            this.from = from;
        }

        public int getAck() {
            return ack;
        }

        public int getFrom() {
            return from;
        }

        public boolean isExist() {
            return exist;
        }

        @Override
        public String toString() {
            if (exist)
                return ack + " " + from;
            else
                return "false";
        }
    }

    public MyService(final int port, @NotNull final Set<String> entireTopology, MyDAO dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port),0);
        this.dao = dao;

        createContextStatus();
        createContextEntity();

        this.topology = new HashSet<>(entireTopology);

        Predicate<String> stringPredicate = p-> p.contains(Integer.toString(port));
        this.topology.removeIf(stringPredicate);
    }

    private void createContextStatus() {
        this.server.createContext("/v0/status", http -> {
            final String response = "ONLINE";
            http.sendResponseHeaders(200, response.length());
            http.getResponseBody().write(response.getBytes());
            http.close();
        });
    }

    private void createContextEntity(){
        this.server.createContext("/v0/entity", http -> {
            final String id = extractId(http.getRequestURI().getQuery());
            final Replicas replicas = extractReplicas(http.getRequestURI().getQuery());

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
                        for (int j = requestStream.read(buffer); j != -1; j = requestStream.read(buffer)){
                            baos.write(buffer,0, j);
                        }
                        baos.flush();
                        dao.upsert(id, baos.toByteArray());
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

    //TODO work is underway
    private byte[] sendRequest() {
        String params = "id=20b5d2c5d842e3a&replicas=2/2";
        byte[] data = null;
        InputStream is = null;

        try {

            URL url = new URL(topology.iterator().next() + "/v0/entity" + params);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setDoInput(true);

            conn.connect();
            int responseCode= conn.getResponseCode();

            System.out.println(123123123);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            is = conn.getInputStream();
            byte[] buffer = new byte[8192]; // Такого вот размера буфер
            // Далее, например, вот так читаем ответ
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            data = baos.toByteArray();

        } catch (Exception e) {
            System.out.println("Test:  " + e.toString());
        } finally {
            try {
                if (is != null)
                    is.close();
            } catch (Exception ex) {}
        }
        return data;
    }

    @NotNull
    private static Replicas extractReplicas(@NotNull final String query) {
        if (!query.contains(REPLICAS_PREFIX)) {
            return new Replicas(false);
        }

        if(!Pattern.matches("^(.)*" + REPLICAS_PREFIX + "([0-9])*" + SLASH + "([0-9])*$", query)) {
            throw new IllegalArgumentException("Wrong replicas");
        }

        String partsOfReplicas[] = query.substring(
                query.indexOf(REPLICAS_PREFIX) + REPLICAS_PREFIX.length()).split(SLASH);

        return new Replicas(Integer.valueOf(partsOfReplicas[0]),Integer.valueOf(partsOfReplicas[1]));
    }

    @NotNull
    private static String extractId(@NotNull final String query) {
        if(!query.startsWith(ID_PREFIX)) {
            throw new IllegalArgumentException("Wrong id");
        }

        int indexOfLastIDSymbol = query.length();

        if (query.contains(REPLICAS_PREFIX)) {
            indexOfLastIDSymbol = query.indexOf(REPLICAS_PREFIX);
        }

        return query.substring(ID_PREFIX.length(), indexOfLastIDSymbol);
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
