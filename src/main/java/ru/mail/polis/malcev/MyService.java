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

    @NotNull
    private final String address;

    private static final String ID_PREFIX = "id=";
    private static final String REPLICAS_PREFIX = "&replicas=";
    private static final String SLASH = "/";

    private static final int OK = 200;
    private static final int CREATED = 201;
    private static final int ACCEPTED = 202;
    private static final int BAD_REQUEST = 400;
    private static final int NOT_FOUND = 404;
    private static final int METHOD_NOT_ALLOWED = 405;
    private static final int GATEWAY_TIMEOUT = 504;

    private static final String GET_REQUEST = "GET";
    private static final String DELETE_REQUEST = "DELETE";
    private static final String PUT_REQUEST = "PUT";

    private final Executor executorOfStatus = Executors.newCachedThreadPool();
    private final Executor executorOfEntity = Executors.newCachedThreadPool();
    private final Executor executorOfInner = Executors.newSingleThreadExecutor();
    private final Executor executorOfStart = Executors.newSingleThreadExecutor();

    private final Set<NotRespondingOnTheRequest> setOfNotRespondingOnTheRequest = new HashSet<>();

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
            return "Replicas{" +
                    "ack=" + ack +
                    ", from=" + from +
                    '}';
        }
    }

    private class InnerRequestAnswer {

        private final int responseCode;
        private final byte[] outputData;
        private final byte[] inputData;
        private final String portWhichGaveAnswer;

        public InnerRequestAnswer(final int responseCode, @NotNull final byte[] outputData,
                                  @NotNull final String portWhichGaveAnswer, @Nullable final byte[] inputData) {
            this.responseCode = responseCode;
            this.outputData = outputData;
            this.inputData = inputData;
            this.portWhichGaveAnswer = portWhichGaveAnswer;
        }

        public int getResponseCode() {
            return responseCode;
        }

        @NotNull
        public byte[] getOutputData() {
            return outputData;
        }

        @NotNull
        public String getPortWhichGaveAnswer() {
            return portWhichGaveAnswer;
        }

        @Nullable
        public byte[] getInputData() {
            return inputData;
        }

        @Override
        public String toString() {
            return "InnerRequestAnswer{" +
                    "responseCode=" + responseCode +
                    ", outputData=" + Arrays.toString(outputData) +
                    ", inputData=" + Arrays.toString(inputData) +
                    ", portWhichGaveAnswer='" + portWhichGaveAnswer + '\'' +
                    '}';
        }
    }

    private static class NotRespondingOnTheRequest implements Serializable{

        public final Set<String> setOfNotRespondingPorts = new HashSet<>();
        public final String requestMethod;
        public final String id;
        public byte[] inputData = new byte[0];

        public NotRespondingOnTheRequest(@NotNull final String requestMethod, @NotNull final String id) {
            this.requestMethod = requestMethod;
            this.id = id;
        }

        public void addPortToSet(@NotNull final String port) {
            setOfNotRespondingPorts.add(port);
        }

        public Set<String> getNotRespondingPorts() {
            return setOfNotRespondingPorts;
        }

        public String getId() {
            return id;
        }

        public String getRequestMethod() {
            return requestMethod;
        }

        public byte[] getInputData() {
            return inputData;
        }

        public void setInputData(@Nullable byte[] inputData) {
            this.inputData = inputData;
        }

        @Override
        public String toString() {
            return "NotRespondingOnTheRequest{" +
                    "setOfNotRespondingPorts=" + setOfNotRespondingPorts +
                    ", requestMethod='" + requestMethod + '\'' +
                    ", id='" + id + '\'' +
                    '}';
        }
    }

    public MyService(final int port, @NotNull final Set<String> topology, @NotNull MyDAO dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        this.topology = sortSet(topology);
        this.address = findPort(port);

        createContextStatus();
        createContextEntity();
        createContextInner();
        createContextStart();
    }

    private String findPort(final int port) {
        for (String str : this.topology)
            if (str.contains(Integer.toString(port))) {
                return str;
            }
        return topology.iterator().next();
    }

    @NotNull
    private static Set<String> sortSet(@NotNull final Set<String> unsortedSet) {
        final Set<String> sortedSet = new TreeSet<>(String::compareTo);
        sortedSet.addAll(unsortedSet);
        return sortedSet;
    }

    private void createContextStart(){
        this.server.createContext("/v0/start",
                http -> executorOfStart.execute(() -> {
                    try {
                        http.sendResponseHeaders(OK, 0);
                        try (final ObjectOutputStream oos = new ObjectOutputStream(http.getResponseBody())) {
                            oos.writeObject(setOfNotRespondingOnTheRequest);
                        }

                    } catch (IOException ex) {
                        System.out.println("Start context: " + ex);

                    } finally {
                        http.close();
                    }
                }));
    }

    private void createContextStatus(){
        this.server.createContext("/v0/status",
                http -> executorOfStatus.execute(() -> {
                    try {
                        final String response = "ONLINE";
                        http.sendResponseHeaders(OK, response.length());
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

                        final Iterator<String> iterator = topology.iterator();
                        InnerRequestAnswer ira;
                        final List<InnerRequestAnswer> listIRA = new ArrayList<>();

                        switch (http.getRequestMethod()){
                            case GET_REQUEST :
                                System.out.println(GET_REQUEST);
                                for (int i = 0; i < topology.size() && i < replicas.getFrom(); i++) {
                                    ira = sendInnerRequest(id, iterator.next(), GET_REQUEST, null);
                                    listIRA.add(ira);
                                }
                                final int responseCode = processingRequestAnswers(listIRA, replicas, GET_REQUEST, id);
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
                                    listIRA.add(ira);
                                }
                                http.sendResponseHeaders(processingRequestAnswers(listIRA, replicas, DELETE_REQUEST, id), 0);
                                break;

                            case PUT_REQUEST :
                                System.out.println(PUT_REQUEST);
                                try (final InputStream requestStream = http.getRequestBody()){
                                    for (int i = 0; i < topology.size() && i < replicas.getFrom(); i++) {
                                        ira = sendInnerRequest(id, iterator.next(),
                                                PUT_REQUEST, StreamReader.readDataFromStream(requestStream));
                                        listIRA.add(ira);
                                    }
                                    http.sendResponseHeaders(processingRequestAnswers(listIRA, replicas, PUT_REQUEST, id), 0);
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

    private int processingRequestAnswers(@NotNull final List<InnerRequestAnswer> listIRA,
                                         @NotNull final Replicas replicas, @NotNull final String nameOfMethod,
                                         @NotNull final String id) {
        if (listIRA.size() == 0)
            return GATEWAY_TIMEOUT;

        int numberOfSuccessAnswers = 0;
        int numberOfServerErrors = 0;

        int positiveResponseToTheRequest;
        switch (nameOfMethod) {
            case PUT_REQUEST :
                positiveResponseToTheRequest = CREATED;
                break;

            case GET_REQUEST :
                positiveResponseToTheRequest = OK;
                break;

            case DELETE_REQUEST :
                positiveResponseToTheRequest = ACCEPTED;
                break;

            default:
                positiveResponseToTheRequest = METHOD_NOT_ALLOWED;
                break;
        }

        final NotRespondingOnTheRequest nrr = new NotRespondingOnTheRequest(nameOfMethod, id);
        for (InnerRequestAnswer element : listIRA)
            if (element.getResponseCode() == positiveResponseToTheRequest)
                numberOfSuccessAnswers++;
            else if (element.getResponseCode() == GATEWAY_TIMEOUT) {
                numberOfServerErrors++;
                nrr.addPortToSet(element.getPortWhichGaveAnswer());
                nrr.setInputData(element.getInputData());
            }

        if (numberOfServerErrors > 0) {
            setOfNotRespondingOnTheRequest.add(nrr);
        }

        int responseCode;
        if (numberOfSuccessAnswers >= replicas.getAck())
            responseCode = positiveResponseToTheRequest;
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
        final URL url = new URL(port + "/v0/inner?id=" + id);
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod(typeOfRequest);
        conn.setDoOutput(true);
        conn.setDoInput(true);

        byte[] outputDataForGet = new byte[0];

        try {
            switch (typeOfRequest) {
                case GET_REQUEST :
                    try (final InputStream inputStream = conn.getInputStream()) {
                        outputDataForGet = StreamReader.readDataFromStream(inputStream);
                    }
                    break;

                case PUT_REQUEST :
                    if (inputDataForPut != null) {
                        try (final OutputStream outputStream = conn.getOutputStream()) {
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

        return new InnerRequestAnswer(responseCode, outputDataForGet, port, inputDataForPut);
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
        http.sendResponseHeaders(ACCEPTED, 0);
    }

    private void requestMethodPut(@NotNull final HttpExchange http, @NotNull final String id) throws IOException {
        try (final InputStream requestStream = http.getRequestBody()) {
            dao.upsert(id, StreamReader.readDataFromStream(requestStream));
        }
        http.sendResponseHeaders(CREATED, 0);
    }

    @NotNull
    private Replicas extractReplicas(@NotNull final String query) {
        if (!query.contains(REPLICAS_PREFIX)) {
            final int defaultValueOfAck = topology.size()/2 + 1;
            final int defaultValueOfFrom = topology.size();
            return new Replicas(defaultValueOfAck, defaultValueOfFrom);
        }
        if (!Pattern.matches("^(.)*" + REPLICAS_PREFIX + "([0-9])*" + SLASH + "([0-9])*$", query)) {
            throw new IllegalArgumentException("Wrong replicas");
        }
        final String partsOfReplicas[] = query.substring(
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

    private void sendRequestForRecovery(@NotNull final String node) {
        try {
            final URL url = new URL(node + "/v0/start");
            final HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            conn.setDoInput(true);

            Set<NotRespondingOnTheRequest> setOfReceivedMissingCommands;

            try (final ObjectInputStream ois = new ObjectInputStream(conn.getInputStream())) {
                setOfReceivedMissingCommands = (Set<NotRespondingOnTheRequest>) ois.readObject();
            }
            if (!setOfReceivedMissingCommands.isEmpty()) {
                for (NotRespondingOnTheRequest element : setOfReceivedMissingCommands) {
                    if(element.getNotRespondingPorts().contains(address)) {
                        sendInnerRequest(element.id, address,element.requestMethod, element.getInputData());
                    }
                }
            }
            conn.disconnect();

        } catch (Exception ex) {
            System.out.println("sendRequestForRecovery: "+ex);
        }
    }

    @Override
    public void start() {
        this.server.start();

        for (String node : topology)
            sendRequestForRecovery(node);
    }

    @Override
    public void stop() {
        this.server.stop(0);
    }

}