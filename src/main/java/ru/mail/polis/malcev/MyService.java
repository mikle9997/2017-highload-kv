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

    private static final int BAD_REQUEST = 400;
    private static final int OK = 200;
    private static final int CREATED = 201;
    private static final int NOT_FOUND = 404;
    private static final int METHOD_NOT_ALLOWED = 405;
    private static final int GATEWAY_TIMEOUT = 504;

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
                        http.sendResponseHeaders(OK,response.length());
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
                            http.sendResponseHeaders(BAD_REQUEST,0);
                            http.close();
                            return;
                        }

                        Iterator<String> iterator = topology.iterator();
                        InnerRequestAnswer ira = new InnerRequestAnswer(GATEWAY_TIMEOUT, new byte[0]);
                        List<InnerRequestAnswer> listIRA = new ArrayList<>();

                        switch (http.getRequestMethod()){
                            case GET_REQUEST :
                                System.out.println(GET_REQUEST);
                                for (int i = 0; i < topology.size() && i < replicas.getFrom(); i++) {
                                    ira = sendInnerRequest(id, iterator.next(), GET_REQUEST, null);
                                    listIRA.add(ira);
                                }
                                System.out.println(listIRA);
                                int responseCode = detectResponseCode(listIRA, replicas, GET_REQUEST);
                                http.sendResponseHeaders(responseCode, 0);
                                if (responseCode == OK) {
                                    byte[] outputValue = new byte[0];
                                    for (InnerRequestAnswer element : listIRA)
                                        if (element.getOutputData().length != 0)
                                            outputValue = element.getOutputData();

                                    http.getResponseBody().write(outputValue);
                                }
                                break;

                            case DELETE_REQUEST :
                                System.out.println(DELETE_REQUEST);
                                for (int i = 0; i < topology.size() && i < replicas.getFrom(); i++) {
                                    ira = sendInnerRequest(id, iterator.next(), DELETE_REQUEST, null);
                                }
                                http.sendResponseHeaders(ira.getResponseCode(), 0);
                                break;

                            case PUT_REQUEST :
                                System.out.println(PUT_REQUEST);
                                try (InputStream requestStream = http.getRequestBody()){
                                    for (int i = 0; i < topology.size() && i < replicas.getFrom(); i++) {
                                        ira = sendInnerRequest(id, iterator.next(),
                                                PUT_REQUEST, StreamReader.readDataFromStream(requestStream));
                                        listIRA.add(ira);
                                    }
                                    System.out.println(listIRA);
                                    http.sendResponseHeaders(detectResponseCode(listIRA, replicas, PUT_REQUEST), 0);
                                }
                                break;
                            default:
                                http.sendResponseHeaders(METHOD_NOT_ALLOWED, 0);
                                break;
                        }
                        http.close();

                    } catch (IOException ex) {
                        System.out.println(ex);
                    }
                }));
    }

    private int detectResponseCode(@NotNull final List<InnerRequestAnswer> listIRA,
                                   @NotNull final Replicas replicas, @NotNull final String nameOfMethod) {
        int numberOfSuccessAnswers = 0;
        int numberOfServerErrors = 0;

        int codeOfMethod;
        switch (nameOfMethod) {
            case "PUT" :
                codeOfMethod = CREATED;
                break;

            case "GET" :
                codeOfMethod = OK;
                break;

            default:
                codeOfMethod = 0;
        }

        for (InnerRequestAnswer element : listIRA)
            if (element.getResponseCode() == codeOfMethod)
                numberOfSuccessAnswers++;
            else if (element.getResponseCode() == GATEWAY_TIMEOUT)
                numberOfServerErrors++;

        int responseCode;
        if (numberOfSuccessAnswers >= replicas.getAck())
            responseCode = codeOfMethod;
        else if (numberOfServerErrors == 0)
            responseCode = NOT_FOUND;
        else
            responseCode = GATEWAY_TIMEOUT;

        return responseCode;
    }

    private void createContextInner() {
        this.server.createContext("/v0/inner",
                (HttpExchange http) -> executorOfInner.execute(() -> {
                    try {
                        final String id = extractId(http.getRequestURI().getQuery());

                        if ("".equals(id)) {
                            http.sendResponseHeaders(BAD_REQUEST,0);
                            http.close();
                            return;
                        }

                        switch (http.getRequestMethod()) {
                            case GET_REQUEST :
                                System.out.println(GET_REQUEST + " inner");
                                requestMethodGet(http, id);
                                break;

                            case DELETE_REQUEST :
                                System.out.println(DELETE_REQUEST + " inner");
                                requestMethodDelete(http, id);
                                break;

                            case PUT_REQUEST :
                                System.out.println(PUT_REQUEST + " inner");
                                requestMethodPut(http, id);
                                break;

                            default:
                                http.sendResponseHeaders(METHOD_NOT_ALLOWED,0);
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
            switch (typeOfRequest) {
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

        int responseCode = GATEWAY_TIMEOUT;
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
            http.sendResponseHeaders(OK, getValue.length);
            http.getResponseBody().write(getValue);

        } catch (NoSuchElementException | IOException e) {
            http.sendResponseHeaders(NOT_FOUND,0);
        }
    }

    private void requestMethodDelete(@NotNull final HttpExchange http, @NotNull final String id) throws IOException {
        dao.delete(id);
        http.sendResponseHeaders(202, 0);
    }

    private void requestMethodPut(@NotNull final HttpExchange http, @NotNull final String id) throws IOException {
        try (InputStream requestStream = http.getRequestBody()) {
            dao.upsert(id, StreamReader.readDataFromStream(requestStream));
        }
        http.sendResponseHeaders(CREATED, 0);
    }

    @NotNull
    private Replicas extractReplicas(@NotNull final String query) {
        if (!query.contains(REPLICAS_PREFIX)) {
            int defaultValueOfAck = topology.size()/2 + 1;
            int defaultValueOfFrom = topology.size();
            return new Replicas(defaultValueOfAck, defaultValueOfFrom);
        }
        if (!Pattern.matches("^(.)*" + REPLICAS_PREFIX + "([0-9])*" + SLASH + "([0-9])*$", query)) {
            throw new IllegalArgumentException("Wrong replicas");
        }
        String partsOfReplicas[] = query.substring(
                query.indexOf(REPLICAS_PREFIX) + REPLICAS_PREFIX.length()).split(SLASH);

        return new Replicas(Integer.valueOf(partsOfReplicas[0]),Integer.valueOf(partsOfReplicas[1]));
    }

    @NotNull
    private String extractId(@NotNull final String query) {
        if (!query.startsWith(ID_PREFIX)) {
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