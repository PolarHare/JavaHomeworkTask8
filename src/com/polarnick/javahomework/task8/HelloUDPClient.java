package com.polarnick.javahomework.task8;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;

/**
 * @author Polyarnyi Nikolay
 */
public class HelloUDPClient {

    private static final String USAGE = "Usage:\n   HelloUDPClient [host] [serverPort] [taskPrefix]";
    private static final int THREADS_COUNT = 10;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println(USAGE);
            return;
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String prefix = args[2];
        if (prefix.getBytes().length > Utils.MAX_BUFFER_SIZE / 2) {
            throw new IllegalArgumentException("TaskPrefix is too long!");
        }
        for (int i = 1; i <= THREADS_COUNT; i++) {
            new Thread(new ClientSender(host, port, prefix, i), "Client #" + i).start();
        }
    }

    private static class ClientSender implements Runnable {

        private final String host;
        private final int port;
        private final String prefix;
        private final int clientId;

        public ClientSender(String host, int port, String prefix, int clientId) {
            this.host = host;
            this.port = port;
            this.prefix = prefix;
            this.clientId = clientId;
        }

        private int nextRequestId = 1;

        private int getNextRequestId() {
            int id = nextRequestId;
            ++nextRequestId;
            return id;
        }

        @Override
        public void run() {
            try (DatagramSocket socket = new DatagramSocket()) {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        long start = System.currentTimeMillis();
                        int requestId = getNextRequestId();
                        String result = executeRequest(requestId, socket);
                        long elapsed = System.currentTimeMillis() - start;
                        log("(response time = " + elapsed + " ms) result for requestId =\t" + requestId + ": " + result);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        private String executeRequest(int requestId, DatagramSocket socket) throws IOException, InterruptedException {
            String request = generateRequest(requestId);
            byte[] requestBytes = request.getBytes();

            ByteBuffer buffer = ByteBuffer.allocate(4 * 4 + requestBytes.length);
            buffer.putInt(Utils.COMMAND_NEW_MESSAGE);
            buffer.putInt(clientId);
            buffer.putInt(requestId);
            buffer.putInt(requestBytes.length);
            buffer.put(requestBytes);
            byte[] data = buffer.array();

            DatagramPacket packet = new DatagramPacket(data, data.length);
            packet.setSocketAddress(new InetSocketAddress(host, port));
            int attemptsNum = 0;
            while (!Thread.currentThread().isInterrupted()) {
                socket.send(packet);
                attemptsNum++;
                try {
                    long timeOfSending = System.currentTimeMillis();
                    log("package sent requestId =\t" + requestId
                            + (attemptsNum == 1 ? "" : "\tattempt #" + attemptsNum));
                    while (!Thread.currentThread().isInterrupted()) {
                        long timeout = Utils.TIMEOUT - Utils.getPassedTimeFrom(timeOfSending);
                        ServerResponse response = listenForResponse(socket, Math.max(0, timeout));
                        if (response == null) {
                            break;
                        }
                        confirmResult(socket, response);

                        if (response.getRequestId() == requestId) {
                            return response.getResponse();
                        }
                        if (Utils.getPassedTimeFrom(timeOfSending) > Utils.TIMEOUT) {
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            throw new InterruptedException();
        }

        private void confirmResult(DatagramSocket socket, ServerResponse response) throws IOException {
            byte[] data;
            ByteBuffer buff = ByteBuffer.allocate(4 + 4 + 4);
            buff.putInt(Utils.COMMAND_GOT_RESULT);
            buff.putInt(clientId);
            buff.putInt(response.getRequestId());
            data = buff.array();
            socket.send(new DatagramPacket(data, data.length, new InetSocketAddress(host, port)));
        }

        private ServerResponse listenForResponse(DatagramSocket socket, long timeout) throws InterruptedException {
            try {
                socket.setSoTimeout((int) timeout);
                byte[] buff = new byte[Utils.MAX_BUFFER_SIZE];
                DatagramPacket packet = new DatagramPacket(buff, buff.length);
                socket.receive(packet);

                ByteBuffer buffer = ByteBuffer.wrap(buff, 0, packet.getLength());
                int commandType = buffer.getInt();
                if (commandType == Utils.COMMAND_RESULT) {
                    int clientId = buffer.getInt();
                    int requestId = buffer.getInt();
                    int dataSize = buffer.getInt();
                    byte[] data = new byte[dataSize];
                    buffer.get(data, 0, dataSize);
                    String result = new String(data);
                    return new ServerResponse(result, requestId);
                } else {
                    throw new IllegalStateException("Unsupported command type = " + commandType);
                }
            } catch (SocketTimeoutException e) {
                return null;
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        public String generateRequest(int requestId) {
            return prefix + "_" + clientId + "_" + requestId;
        }

        public void log(String message) {
            System.out.println("Client \t" + clientId + ": " + message);
        }

        public static class ServerResponse {
            private final String response;
            private final int requestId;

            public ServerResponse(String response, int requestId) {
                this.response = response;
                this.requestId = requestId;
            }

            public int getRequestId() {
                return requestId;
            }

            public String getResponse() {
                return response;
            }
        }

    }

}
