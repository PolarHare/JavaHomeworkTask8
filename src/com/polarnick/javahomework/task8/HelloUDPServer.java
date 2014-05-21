package com.polarnick.javahomework.task8;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Polyarnyi Nikolay
 */
public class HelloUDPServer {

    private static final String USAGE = "USAGE:\n   HelloUDPServer [serverPort]";
    private static final int THREADS_COUNT = 17;
    private static final int MAX_TASK_EXECUTION_TIME = 23910;
    private static final int CACHE_SIZE = 1024;

    private final int port;

    private DatagramSocket socket;
    private BlockingQueue<DatagramPacket> newMessagesQueue;
    private Map<Integer, BlockingQueue<DatagramPacket>> newPacketsByClientId;
    private ConcurrentLinkedHashMap<String, String> responseCache;

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
        try {
            responseCache = new ConcurrentLinkedHashMap.Builder<String, String>()
                    .maximumWeightedCapacity(CACHE_SIZE).build();
            newMessagesQueue = new LinkedBlockingQueue<>();
            newPacketsByClientId = new ConcurrentHashMap<>();
            socket = new DatagramSocket(port);
            listeners = new ArrayList<>(THREADS_COUNT);
            for (int i = 0; i < THREADS_COUNT; i++) {
                Thread listener = new Thread(new ServerListener(i), "Server #" + i);
                listener.start();
                listeners.add(listener);
            }
            while (!Thread.currentThread().isInterrupted()) {
                DatagramPacket packet = new DatagramPacket(new byte[Utils.MAX_BUFFER_SIZE], Utils.MAX_BUFFER_SIZE);
                socket.receive(packet);
                registerPacket(packet);
            }
            for (Thread listener : listeners) {
                listener.interrupt();
            }
            for (Thread listener : listeners) {
                listener.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            if (socket != null) {
                socket.close();
            }
        }
    }

    private void registerPacket(DatagramPacket packet) {
        ByteBuffer buff = ByteBuffer.wrap(packet.getData(), packet.getOffset(), packet.getLength());
        int commandType = buff.getInt();
        int clientId = buff.getInt();
        if (commandType == Utils.COMMAND_NEW_MESSAGE) {
            if (!newPacketsByClientId.containsKey(clientId)) {
                newPacketsByClientId.put(clientId, new LinkedBlockingQueue<>());
            }
            newMessagesQueue.add(packet);
        } else {
            newPacketsByClientId.get(clientId).add(packet);
        }
    }

    private DatagramPacket acceptNewClient() throws IOException, InterruptedException {
        return newMessagesQueue.take();
    }

    private DatagramPacket receiveDatagramPacket(int clientId, long timeout) throws IOException, InterruptedException {
        return newPacketsByClientId.get(clientId).poll(timeout, TimeUnit.MILLISECONDS);
    }

    private class ServerListener implements Runnable {

        private final int id;

        public ServerListener(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        DatagramPacket packet = acceptNewClient();
                        handle(packet);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
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
                processRequest(clientId, requestId, message, packet);
            } else if (commandType == Utils.COMMAND_GOT_RESULT) {
                int clientId = buff.getInt();
                int requestId = buff.getInt();
                warn("got result confirmation {clientId =\t" + clientId + ", requestId =\t" + requestId + "}");
            } else {
                throw new IllegalStateException("Unsupported command type = " + commandType);
            }
        }

        private void processRequest(int clientId, int requestId, String message, DatagramPacket packet) throws IOException {
            String response = generateResponse(message);
            byte[] responseBytes = response.getBytes();
            boolean clientGotTheResult = false;
            int attemptsNum = 0;
            while (!clientGotTheResult && !Thread.currentThread().isInterrupted()) {
                sendResponse(clientId, requestId, packet, responseBytes);
                attemptsNum++;
                log("package sent {clientId =\t" + clientId + ", requestId =\t" + requestId + ", response =\t'" + response + "'}"
                        + (attemptsNum == 1 ? "" : " attempt #" + attemptsNum));
                long timeOfSending = System.currentTimeMillis();

                while (!clientGotTheResult && !Thread.currentThread().isInterrupted()) {
                    long timeout = Utils.TIMEOUT - Utils.getPassedTimeFrom(timeOfSending);

                    try {
                        DatagramPacket packetFromClient = receiveDatagramPacket(clientId, timeout);
                        if (packetFromClient == null) {//timeout
                            break;
                        }

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
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    if (Utils.getPassedTimeFrom(timeOfSending) > Utils.TIMEOUT) {
                        break;
                    }
                }
            }
        }

        private void sendResponse(int clientId, int requestId, DatagramPacket packet, byte[] responseBytes) throws IOException {
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
            String result = responseCache.get(request);
            if (result == null) {
                Random r = new Random();
                int timeOfPureExecuting = r.nextInt(MAX_TASK_EXECUTION_TIME);
                Utils.sleepWithThreadInterrupting(timeOfPureExecuting);
                result = "Hello, " + request + " (time of executing = " + timeOfPureExecuting + " ms)";
                responseCache.put(request, result);
            }
            return result;
        }
    }

}
