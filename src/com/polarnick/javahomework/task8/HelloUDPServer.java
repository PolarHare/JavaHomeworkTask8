package com.polarnick.javahomework.task8;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author Polyarnyi Nikolay
 */
public class HelloUDPServer {

    private static final String USAGE = "USAGE:\n   HelloUDPServer [serverPort]";
    private static final int THREADS_COUNT = 1;
    private static final int MAX_TASK_RESPONSE_DELAY = 2391;

    private final int port;

    public static void main(String[] args) throws SocketException {
        if (args.length < 1) {
            System.out.println(USAGE);
            return;
        }
        int port = Integer.parseInt(args[0]);
        HelloUDPServer server = new HelloUDPServer(port);
        server.start();
    }

    public HelloUDPServer(int port) {
        this.port = port;
    }

    private void start() {
        List<Thread> listeners;
        try (DatagramSocket socket = new DatagramSocket(port)) {
            listeners = new ArrayList<>(THREADS_COUNT);
            for (int i = 0; i < THREADS_COUNT; i++) {
                Thread listener = new Thread(new ServerListener(socket, i), "Server #" + i);
                listener.start();
                listeners.add(listener);
            }
            for (Thread listener : listeners) {
                listener.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (SocketException e) {
            throw new IllegalStateException(e);
        }
    }

    private class ServerListener implements Runnable {

        private final DatagramSocket socket;
        private final int id;

        public ServerListener(DatagramSocket socket, int id) {
            this.socket = socket;
            this.id = id;
        }

        @Override
        public void run() {
            try {
                DatagramPacket packet = new DatagramPacket(new byte[Utils.MAX_BUFFER_SIZE], Utils.MAX_BUFFER_SIZE);
                while (!Thread.currentThread().isInterrupted()) {
                    socket.receive(packet);
                    handle(packet);
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        private void handle(DatagramPacket packet) throws IOException {
            ByteBuffer buff = ByteBuffer.wrap(packet.getData(), packet.getOffset(), packet.getLength());
            int commandType = buff.getInt();
            if (commandType == Utils.COMMAND_NEW_MESSAGE) {
                int clientId = buff.getInt();
                int requestId = buff.getInt();
                int dataLength = buff.getInt();
                byte[] data = new byte[dataLength];
                buff.get(data, 0, dataLength);
                String message = new String(data);
                log("got message {clientId =\t" + clientId + ", requestId =\t" + requestId + ", request =\t'" + message + "'}");
                processRequest(clientId, requestId, message, packet, socket);
            } else if (commandType == Utils.COMMAND_GOT_RESULT) {
                int clientId = buff.getInt();
                int requestId = buff.getInt();
                warn("got result confirmation {clientId =\t" + clientId + ", requestId =\t" + requestId + "}");
            } else {
                throw new IllegalStateException("Unsupported command type = " + commandType);
            }
        }

        private void processRequest(int clientId, int requestId, String message, DatagramPacket packet, DatagramSocket socket) throws IOException {
            String response = generateResponse(message);
            byte[] responseBytes = response.getBytes();
            boolean clientGotTheResult = false;
            int attemptsNum = 0;
            while (!clientGotTheResult && !Thread.currentThread().isInterrupted()) {
                sendResponse(clientId, requestId, packet, socket, responseBytes);
                attemptsNum++;
                log("package sent {clientId =\t" + clientId + ", requestId =\t" + requestId + ", response =\t'" + response + "'}"
                        + (attemptsNum == 1 ? "" : " attempt #" + attemptsNum));
                long timeOfSending = System.currentTimeMillis();

                while (!clientGotTheResult && !Thread.currentThread().isInterrupted()) {
                    long timeout = Utils.TIMEOUT - Utils.getPassedTimeFrom(timeOfSending);

                    try {
                        socket.setSoTimeout((int) timeout);
                        DatagramPacket packetFromClient = new DatagramPacket(new byte[Utils.MAX_BUFFER_SIZE], Utils.MAX_BUFFER_SIZE);
                        socket.receive(packetFromClient);

                        ByteBuffer buff = ByteBuffer.wrap(packetFromClient.getData(), packetFromClient.getOffset(), packetFromClient.getLength());
                        int commandType = buff.getInt();
                        if (commandType == Utils.COMMAND_GOT_RESULT) {
                            int clientIdGot = buff.getInt();
                            int requestIdGot = buff.getInt();
                            if (clientIdGot != clientId) {
                                throw new IllegalStateException("Foreign client with id=" + clientIdGot + " but expected with id=" + clientId);
                            }
                            if (requestIdGot < requestId) {
                                log("old result confirmation requestId =\t" + requestIdGot);
                            } else if (requestIdGot > requestId) {
                                throw new IllegalStateException("Result confirmation from future! RequestId =\t" + requestIdGot + " but expected with requestId =" + requestId);
                            } else if (requestIdGot == requestId) {
                                clientGotTheResult = true;
                            }
                        } else if (commandType == Utils.COMMAND_NEW_MESSAGE) {
                            int clientIdGot = buff.getInt();
                            int requestIdGot = buff.getInt();
                            if (clientIdGot != clientId || requestIdGot != requestId) {
                                throw new IllegalStateException("Unexpected new request command " +
                                        "clientId =\t" + clientIdGot + " but expected clientId =\t" + clientId
                                        + "  and requestId =\t" + requestIdGot + " but expected requestId =\t" + requestId);
                            }
                        } else {
                            throw new IllegalStateException("Unexpected command type = " + commandType);
                        }
                    } catch (SocketTimeoutException e) {
                        break;
                    }

                    if (Utils.getPassedTimeFrom(timeOfSending) > Utils.TIMEOUT) {
                        break;
                    }
                }
            }
        }

        private void sendResponse(int clientId, int requestId, DatagramPacket packet, DatagramSocket socket, byte[] responseBytes) throws IOException {
            ByteBuffer buff = ByteBuffer.allocate(4 * 4 + responseBytes.length);
            buff.putInt(Utils.COMMAND_RESULT);
            buff.putInt(clientId);
            buff.putInt(requestId);
            buff.putInt(responseBytes.length);
            buff.put(responseBytes);
            byte[] responsePacketBytes = buff.array();
            DatagramPacket responsePacket = new DatagramPacket(responsePacketBytes, responsePacketBytes.length, packet.getAddress(), packet.getPort());
            socket.send(responsePacket);
        }

        public void log(String message) {
            System.out.println("Server listener \t" + id + ": " + message);
        }

        public void warn(String message) {
            System.out.print("[WARN] ");
            log(message);
        }

        private String generateResponse(String request) {
            Random r = new Random();
            Utils.sleepWithThreadInterrupting(r.nextInt(MAX_TASK_RESPONSE_DELAY));
            return "Hello, " + request;
        }
    }

}
