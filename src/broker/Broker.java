package broker;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import common.InterBrokerMessage;
import common.Address;
import common.LocalIP;

public class Broker {
    private int port;
    public Address brokerAddress;
    Map<String, List<Integer>> queues = new ConcurrentHashMap<>();
    public Map<String, Map<String, Integer>> clientOffsets = new ConcurrentHashMap<>(); // clientId -> (queueName -> offset)
    private MulticastSocket brokerMulticastSocket;
    private InetSocketAddress brokerGroup;
    private List<String> knownBrokers;
    public Map<String, Address> queueAddressMap = new ConcurrentHashMap<>();
    public Map<String, Map<String, Integer>> clientOffsetAddressMap = new ConcurrentHashMap<>();

    private static final String CLIENT_MULTICAST_ADDRESS = "239.255.0.2";
    private static final int CLIENT_MULTICAST_PORT = 5010;
    private static final String BROKER_MULTICAST_ADDRESS = "239.255.0.1";
    private static final int BROKER_MULTICAST_PORT = 5020;
    private static final int DISCOVERY_TIMEOUT = 1000; // 1 second timeout

    public Broker(int port) throws IOException {
        this.port = port;
        this.knownBrokers = new ArrayList<>();
        this.brokerAddress = new Address(LocalIP.getLocalIP().toString(),port);
        startMulticastListener();
        createBrokerMulticastSocket();
        startBrokerMulticastListener();
        startPeriodicPing();
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("[INFO]: [Broker: " + port +  "] Started");
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
        InterBrokerMessage interBrokerMessage = new InterBrokerMessage();
        interBrokerMessage.setMessageType("PING");
        interBrokerMessage.setPort(port);
        byte[] messageBytes = interBrokerMessage.serializeToBytes();
        DatagramPacket datagramPacket = new DatagramPacket(messageBytes, messageBytes.length, brokerGroup.getAddress(), brokerGroup.getPort());
        brokerMulticastSocket.send(datagramPacket);
        System.out.println("[INFO]: [Broker: " + port +  "] Sent healthcheck to " + BROKER_MULTICAST_ADDRESS + ":" + BROKER_MULTICAST_PORT);
    }

    public void updateQueueAddressMap(String queueName) throws IOException {
        InterBrokerMessage interBrokerMessage = new InterBrokerMessage();
        interBrokerMessage.setQueueName(queueName);
        interBrokerMessage.setLeader(brokerAddress);
        interBrokerMessage.setMessageType("NEW_QUEUE");
        byte[] messageBytes = interBrokerMessage.serializeToBytes();
        DatagramPacket packet = new DatagramPacket(messageBytes, messageBytes.length, brokerGroup.getAddress(), BROKER_MULTICAST_PORT);
        brokerMulticastSocket.send(packet);
        System.out.println("[INFO]: [Broker: " + port +  "] Updated queue address map added " + queueName + " leader: " + brokerAddress);
    }

    private void sendPingResponse() throws IOException {
        InterBrokerMessage interBrokerMessage = new InterBrokerMessage();
        interBrokerMessage.setMessageType("ACK");
        interBrokerMessage.setPort(port);
        byte[] messageBytes = interBrokerMessage.serializeToBytes();
        DatagramPacket datagramPacket = new DatagramPacket(messageBytes, messageBytes.length, brokerGroup.getAddress(), brokerGroup.getPort());
        brokerMulticastSocket.send(datagramPacket);
        System.out.println("[INFO]: [Broker: " + port + "] Responded to ping request with " + interBrokerMessage.getPort());
    }

    private void startMulticastListener() {
        new Thread(() -> {
            try {
                MulticastSocket multicastSocket = new MulticastSocket(CLIENT_MULTICAST_PORT);
                InetAddress mcastaddr = InetAddress.getByName(CLIENT_MULTICAST_ADDRESS);
                NetworkInterface netIf = NetworkInterface.getByInetAddress(LocalIP.getLocalIP());
                multicastSocket.joinGroup(new InetSocketAddress(mcastaddr, CLIENT_MULTICAST_PORT), netIf);
                System.out.println("[INFO]: [Broker: " + port +  "] Listening client discovery on " + CLIENT_MULTICAST_ADDRESS + ":" + CLIENT_MULTICAST_PORT);


                while (true) {
                    byte[] buffer = new byte[1024];
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
                        System.out.println("[INFO]: [Broker: " + port +  "] Responded to discovery request with " + response);
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
        InetSocketAddress group = new InetSocketAddress(mcastaddr, BROKER_MULTICAST_PORT);
        // We don't know why this works, but if we try to put a valid network interface, it doesnt work sometimes (randomly we guess)
        brokerMulticastSocket.joinGroup(group, null);
        brokerMulticastSocket.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, false);
        brokerMulticastSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        brokerGroup = group;
    }



    private void startBrokerMulticastListener() {
        new Thread(() -> {
            try {
                System.out.println("[INFO]: [Broker: " + port +  "] Listening other brokers on " + BROKER_MULTICAST_ADDRESS + ":" + BROKER_MULTICAST_PORT);
                while (true) {
                    byte[] buffer = new byte[1024];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    brokerMulticastSocket.receive(packet);
                    InterBrokerMessage receivedInterBrokerMessage = InterBrokerMessage.deserializeFromBytes(packet.getData());
                    if (receivedInterBrokerMessage.getMessageType().equals("PING") && !((packet.getAddress().equals(LocalIP.getLocalIP())) && (receivedInterBrokerMessage.getPort()) == (port))) {
                        sendPingResponse();
                        registerBroker(packet.getAddress().getHostAddress(),receivedInterBrokerMessage.getPort().toString());
                    }
                    else if (receivedInterBrokerMessage.getMessageType().equals("NEW_QUEUE")) {
                        queueAddressMap.put(receivedInterBrokerMessage.getQueueName(), receivedInterBrokerMessage.getLeader());
                        sendPingResponse();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    private void registerBroker(String host, String port) {
        String brokerInfo = host + ":" + port;
        System.out.println("[INFO]: [Broker: " + this.port +  "] Received response from " + brokerInfo);
        if (!knownBrokers.contains(brokerInfo)) {
            knownBrokers.add(brokerInfo);
            System.out.println("[INFO]: [Broker: " + this.port +  "] Registered broker " + brokerInfo);
        }
    }
}

