package broker;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import common.LocalIP;

public class Broker {
    private int port;
    Map<String, List<Integer>> queues = new ConcurrentHashMap<>();
    public Map<String, Map<String, Integer>> clientOffsets = new ConcurrentHashMap<>(); // clientId -> (queueName -> offset)
    private MulticastSocket brokerMulticastSocket;
    private InetSocketAddress brokerGroup;
    private List<String> knownBrokers;

    private static final String CLIENT_MULTICAST_ADDRESS = "239.255.0.2";
    private static final int CLIENT_MULTICAST_PORT = 5010;
    private static final String BROKER_MULTICAST_ADDRESS = "239.255.0.1";
    private static final int BROKER_MULTICAST_PORT = 5020;
    private static final int DISCOVERY_TIMEOUT = 1000; // 1 second timeout

    public Broker(int port) throws IOException {
        this.port = port;
        this.knownBrokers = new ArrayList<>();
        startMulticastListener();
        createBrokerMulticastSocket();
        startBrokerMulticastListener();
        startPeriodicPing();
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Broker started on port " + port);
            while (true) {
                Socket socket = serverSocket.accept();
                String clientId = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
                System.out.println("Client connected: " + clientId);
                new Thread(new ClientHandler(socket, clientId, this)).start();

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void removeClient(String clientId) {
        clientOffsets.remove(clientId);
        System.out.println("Client disconnected: " + clientId);
    }

    private void startPeriodicPing() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        Random random = new Random();

        scheduler.scheduleAtFixedRate(() -> {
            try {
                Thread.sleep(random.nextInt(2000)); // Random delay between 0-10s
                sendPingRequest();
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }, 0, 2, TimeUnit.SECONDS);
    }

    private void sendPingRequest() throws IOException {
        String message = "PING_BROKERS" + System.lineSeparator() + port;
        byte[] messageBytes = message.getBytes();
        DatagramPacket packet = new DatagramPacket(messageBytes, messageBytes.length, brokerGroup.getAddress(), BROKER_MULTICAST_PORT);
        brokerMulticastSocket.send(packet);
        System.out.println("Sent ping request to " + BROKER_MULTICAST_ADDRESS + ":" + BROKER_MULTICAST_PORT);

    }

    private void sendPingResponse() throws IOException {
        String response = String.valueOf(port);
        byte[] responseBytes = response.getBytes();
        DatagramPacket responsePacket = new DatagramPacket(responseBytes, responseBytes.length, brokerGroup.getAddress(), BROKER_MULTICAST_PORT);
        brokerMulticastSocket.send(responsePacket);
        System.out.println("Responded to ping request with " + response);
    }

    private void startMulticastListener() {
        new Thread(() -> {
            try {
                MulticastSocket multicastSocket = new MulticastSocket(CLIENT_MULTICAST_PORT);
                InetAddress mcastaddr = InetAddress.getByName(CLIENT_MULTICAST_ADDRESS);
                NetworkInterface netIf = NetworkInterface.getByInetAddress(InetAddress.getLocalHost());
                multicastSocket.joinGroup(new InetSocketAddress(mcastaddr, CLIENT_MULTICAST_PORT), netIf);
                System.out.println("Broker listening for multicast discovery on " + CLIENT_MULTICAST_ADDRESS + ":" + CLIENT_MULTICAST_PORT);


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

    private void createBrokerMulticastSocket() throws IOException {
        brokerMulticastSocket = new MulticastSocket(BROKER_MULTICAST_PORT);
        InetAddress mcastaddr = InetAddress.getByName(BROKER_MULTICAST_ADDRESS);
        NetworkInterface netIf = NetworkInterface.getByName("en0");
        InetSocketAddress group = new InetSocketAddress(mcastaddr, BROKER_MULTICAST_PORT);
        brokerMulticastSocket.joinGroup(group, netIf);
        brokerMulticastSocket.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, false);
        brokerGroup = group;
    }



    private void startBrokerMulticastListener() {
        new Thread(() -> {
            try {
                System.out.println("Broker listening for multicast ping on " + BROKER_MULTICAST_ADDRESS + ":" + BROKER_MULTICAST_PORT);
                while (true) {
                    byte[] buffer = new byte[256];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    brokerMulticastSocket.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength());
                    String[] messageArray = message.split("\n");

                    if (messageArray[0].equals("PING_BROKERS") && !(packet.getAddress().equals(LocalIP.getLocalIP())) && !(Integer.parseInt(messageArray[1]) == (port))) {
                        sendPingResponse();
                        registerBroker(packet.getAddress().getHostAddress(),messageArray[1]);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    private void registerBroker(String host, String port) {
        String brokerInfo = host + ":" + port;
        if (!knownBrokers.contains(brokerInfo)) {
            knownBrokers.add(brokerInfo);
        }
    }
}

