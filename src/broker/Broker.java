package broker;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Broker {
    private int port;
    Map<String, List<Integer>> queues = new ConcurrentHashMap<>();
    public Map<String, Map<String, Integer>> clientOffsets = new ConcurrentHashMap<>(); // clientId -> (queueName -> offset)
    private Set<String> currentClients = ConcurrentHashMap.newKeySet();


    private static final String MULTICAST_ADDRESS = "224.0.0.1";
    private static final int MULTICAST_PORT = 5010;

    public Broker(int port) {
        this.port = port;
        startMulticastListener();
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Broker started on port " + port);
            while (true) {
                Socket socket = serverSocket.accept();
                String clientId = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
                currentClients.add(clientId);
                System.out.println("Client connected: " + clientId);
                new Thread(new ClientHandler(socket, clientId, this)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void removeClient(String clientId) {
        currentClients.remove(clientId);
        clientOffsets.remove(clientId);
        System.out.println("Client disconnected: " + clientId);
    }

    private void startMulticastListener() {
        new Thread(() -> {
            try {
                MulticastSocket multicastSocket = new MulticastSocket(MULTICAST_PORT);
                InetAddress mcastaddr = InetAddress.getByName(MULTICAST_ADDRESS);
                NetworkInterface netIf = NetworkInterface.getByInetAddress(InetAddress.getLocalHost());
                multicastSocket.joinGroup(new InetSocketAddress(mcastaddr, MULTICAST_PORT), netIf);
                System.out.println("Broker listening for multicast discovery on " + MULTICAST_ADDRESS + ":" + MULTICAST_PORT);


                while (true) {
                    byte[] buffer = new byte[256];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    multicastSocket.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength());
                    if ("DISCOVER_BROKERS".equals(message)) {
                        String response = String.valueOf(port);
                        byte[] responseBytes = response.getBytes();
                        DatagramPacket responsePacket = new DatagramPacket(
                                responseBytes, responseBytes.length, packet.getAddress(), packet.getPort()
                        );
                        multicastSocket.send(responsePacket);
                        System.out.println("Responded to discovery request with " + response);
                    }

                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }
}

