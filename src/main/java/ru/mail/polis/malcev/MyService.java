package ru.mail.polis.malcev;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.KVService;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;


public class MyService implements KVService {

    @NotNull
    private final HttpServer server;

    @NotNull
    private final MyDAO dao;

    @NotNull
    private final Set<String> topology;

    private static final String ID_PREFIX = "id=";
    private static final String REPLICAS_PREFIX = "&replicas=";
    private static final String SLASH = "/";

    private static final String GET_REQUEST = "GET";
    private static final String DELETE_REQUEST = "DELETE";
    private static final String PUT_REQUEST = "PUT";

    private final Executor executorOfStatus = Executors.newCachedThreadPool();
    private final Executor executorOfEntity = Executors.newCachedThreadPool();
    private final Executor executorOfInner = Executors.newSingleThreadExecutor();

    private class Replicas {

        private final int ack;

        private final int from;

        public Replicas(final int ack, final int from) {
            this.ack = ack;
            this.from = from;
        }

        public int getAck() {
            return ack;
        }

        public int getFrom() {
            return from;
        }

        @Override
        public String toString() {
            return ack + " " + from;
        }
    }

    private class InnerRequestAnswer {

        private final int responseCode;
        private final byte[] outputData;

        public InnerRequestAnswer(final int responseCode, @NotNull final byte[] outputData) {
            this.responseCode = responseCode;
            this.outputData = outputData;
        }

        public int getResponseCode() {
            return responseCode;
        }

        @NotNull
        public byte[] getOutputData() {
            return outputData;
        }

        @Override
        public String toString() {
            return Integer.toString(responseCode);
        }
    }

    public MyService(final int port, @NotNull final Set<String> entireTopology,@NotNull MyDAO dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port),0);
        this.dao = dao;
        this.topology = sortSet(entireTopology);

        createContextStatus();
        createContextEntity();
        createContextInner();
    }

    @NotNull
    private static Set<String> sortSet(@NotNull final Set<String> unsortedSet) {
        Set<String> sortedSet = new TreeSet<>(String::compareTo);
        sortedSet.addAll(unsortedSet);
        return sortedSet;
    }

    private void createContextStatus(){
        this.server.createContext("/v0/status",
                http -> executorOfStatus.execute(() -> {
                    try {
                        final String response = "ONLINE";
                        http.sendResponseHeaders(200,response.length());
                        http.getResponseBody().write(response.getBytes());

                    } catch (IOException ex) {
                        System.out.println(ex);

                    } finally {
                        http.close();
                    }

                }));
    }

    private void createContextEntity(){
        this.server.createContext("/v0/entity",
                (HttpExchange http) -> executorOfEntity.execute(() -> {
                    try {
                        final String id = extractId(http.getRequestURI().getQuery());
                        final Replicas replicas = extractReplicas(http.getRequestURI().getQuery());

                        if ("".equals(id) || replicas.getAck() > replicas.getFrom() || replicas.getAck() < 1) {
                            http.sendResponseHeaders(400,0);
                            http.close();
                            return;
                        }

                        Iterator<String> iterator = topology.iterator();
                        InnerRequestAnswer ira = new InnerRequestAnswer(504, new byte[0]);
                        List<InnerRequestAnswer> listIRA = new ArrayList<>();
                        int responseCode = 0;

                        switch (http.getRequestMethod()){
                            case GET_REQUEST :
                                System.out.println(GET_REQUEST);
                                ira = sendInnerRequest(id, iterator.next(), GET_REQUEST, null);
//                                for (int i = 0; i < topology.size() && i < replicas.getAck(); i++) {
//                                    ira = sendInnerRequest(id, iterator.next(), GET_REQUEST, null);
//                                    listIRA.add(ira);
//                                }
//                                for (InnerRequestAnswer element : listIRA) { // всё хорошо 200, не нашёл 404, не дойти до сервера 504
//                                    if (element.getResponseCode() == 200)
//                                        responseCode = 200;
//                                    if (element.getResponseCode() == 504 && responseCode != 200) {
//                                        responseCode = 504;
//                                    }
//                                }
                                responseCode = ira.responseCode;
                                http.sendResponseHeaders(responseCode, 0);
                                http.getResponseBody().write(ira.getOutputData());
                                break;

                            case DELETE_REQUEST :
                                System.out.println(DELETE_REQUEST);
                                ira = sendInnerRequest(id, iterator.next(), DELETE_REQUEST, null);

                                http.sendResponseHeaders(ira.getResponseCode(), 0);
                                break;

                            case PUT_REQUEST :
                                System.out.println(PUT_REQUEST);
                                try (InputStream requestStream = http.getRequestBody()){
                                    ira = sendInnerRequest(id, iterator.next(),
                                            PUT_REQUEST, StreamReader.readDataFromStream(requestStream));

                                    http.sendResponseHeaders(ira.getResponseCode(), 0);
                                }
                                break;
                            default:
                                http.sendResponseHeaders(405, 0);
                                break;
                        }
                        http.close();

                    } catch (IOException ex) {
                        System.out.println(ex);
                    }
                }));
    }

    private void createContextInner() {
        this.server.createContext("/v0/inner",
                (HttpExchange http) -> executorOfInner.execute(() -> {
                    try {
                        final String id = extractId(http.getRequestURI().getQuery());

                        if ("".equals(id)) {
                            http.sendResponseHeaders(400,0);
                            http.close();
                            return;
                        }

                        switch (http.getRequestMethod()){
                            case GET_REQUEST :
                                System.out.println(GET_REQUEST);
                                requestMethodGet(http, id);
                                break;

                            case DELETE_REQUEST :
                                System.out.println(DELETE_REQUEST);
                                requestMethodDelete(http, id);
                                break;

                            case PUT_REQUEST :
                                System.out.println(PUT_REQUEST);
                                requestMethodPut(http, id);
                                break;

                            default:
                                http.sendResponseHeaders(405,0);
                                break;
                        }
                        http.close();

                    } catch (IOException ex) {
                        System.out.println(ex);
                    }
                }));
    }

    @NotNull
    private InnerRequestAnswer sendInnerRequest(@NotNull final String id, @NotNull final String port,
                                                @NotNull final String typeOfRequest,
                                                @Nullable byte[] inputDataForPut)  throws IOException {
        URL url = new URL(port + "/v0/inner?id=" + id);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod(typeOfRequest);
        conn.setDoOutput(true);
        conn.setDoInput(true);

        byte[] outputDataForGet = new byte[0];

        try {
            switch (typeOfRequest){
                case GET_REQUEST :
                    try (InputStream inputStream = conn.getInputStream()) {
                        outputDataForGet = StreamReader.readDataFromStream(inputStream);
                    }
                    break;

                case PUT_REQUEST :
                    if (inputDataForPut != null) {


                        try (OutputStream outputStream = conn.getOutputStream()) {
                            outputStream.write(inputDataForPut);
                        }
                    }
                    break;
            }
        } catch (IOException e) {
            //do nothing
        }

        int responseCode = 504;
        try {
            responseCode = conn.getResponseCode();

        } catch (IOException e) {
            //do nothing
        }
        conn.disconnect();

        return new InnerRequestAnswer(responseCode, outputDataForGet);
    }

    private void requestMethodGet(@NotNull final HttpExchange http, @NotNull final String id) throws IOException {
        try {
            final byte[] getValue = dao.get(id);
            http.sendResponseHeaders(200, getValue.length);
            http.getResponseBody().write(getValue);

        } catch (NoSuchElementException | IOException e) {
            http.sendResponseHeaders(404,0);
        }
    }

    private void requestMethodDelete(@NotNull final HttpExchange http, @NotNull final String id) throws IOException {
        dao.delete(id);
        http.sendResponseHeaders(202, 0);
    }

    private void requestMethodPut(@NotNull final HttpExchange http, @NotNull final String id) throws IOException {
        try (InputStream requestStream = http.getRequestBody()){
            dao.upsert(id, StreamReader.readDataFromStream(requestStream));
        }
        http.sendResponseHeaders(201, 0);
    }

    @NotNull
    private Replicas extractReplicas(@NotNull final String query) {
        if (!query.contains(REPLICAS_PREFIX)) {
            int defaultValueOfAck = topology.size()/2 + 1;
            int defaultValueOfFrom = topology.size();
            return new Replicas(defaultValueOfAck, defaultValueOfFrom);
        }
        if(!Pattern.matches("^(.)*" + REPLICAS_PREFIX + "([0-9])*" + SLASH + "([0-9])*$", query)) {
            throw new IllegalArgumentException("Wrong replicas");
        }
        String partsOfReplicas[] = query.substring(
                query.indexOf(REPLICAS_PREFIX) + REPLICAS_PREFIX.length()).split(SLASH);

        return new Replicas(Integer.valueOf(partsOfReplicas[0]),Integer.valueOf(partsOfReplicas[1]));
    }

    @NotNull
    private String extractId(@NotNull final String query) {
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