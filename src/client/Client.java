package client;
import common.Address;
import common.Message;
import common.Operation;
import common.enums.ResponseType;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Client {
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private Scanner scanner;
    private String id;

    private static final String MULTICAST_ADDRESS = "224.0.0.1";
    private static final int MULTICAST_PORT = 5010;
    private static final int DISCOVERY_TIMEOUT = 1000;
    private static final int DISCOVERY_INTERVAL = 500;

    private final TreeSet<Address> brokerCache = new TreeSet<>(
            Comparator.comparing(Address::getHost)
                    .thenComparingInt(Address::getPort)
    );

    private AtomicBoolean runDiscovery = new AtomicBoolean(true);
    private AtomicBoolean isConnected = new AtomicBoolean(false);

    private Thread discoveryThread;

    public Client() {
        scanner = new Scanner(System.in);
        System.out.println("[INFO]: Client started. Performing initial broker discovery.");
        startContinuousDiscovery();

        discoverBrokers(true);
        if (brokerCache.isEmpty()) {
            System.err.println("[ERROR]: No brokers found during initial discovery.");
        }
    }

    private void startContinuousDiscovery() {
        discoveryThread = new Thread(() -> {
            while (runDiscovery.get()) {
                try {
                    Thread.sleep(DISCOVERY_INTERVAL);
                    if (isConnected.get()) {
                        continue;
                    }

                    List<Address> previousBrokers = new ArrayList<>(brokerCache);
                    discoverBrokers(false);
                    compareAndLogBrokerChanges(previousBrokers, brokerCache, true);
                } catch (InterruptedException e) {
                    System.err.println("[INFO]: Discovery thread interrupted.");
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            System.out.println("[INFO]: Discovery thread stopped.");
        });
        discoveryThread.setDaemon(true);
        discoveryThread.start();
    }

    private void compareAndLogBrokerChanges(Collection<Address> previousBrokers, Collection<Address> currentBrokers, boolean showSymbol) {
        Set<Address> previousBrokersSet = new HashSet<>(previousBrokers);
        Set<Address> currentBrokersSet = new HashSet<>(currentBrokers);

        if (previousBrokersSet.equals(currentBrokersSet)) {
            return;
        }

        for (Address broker : currentBrokers) {
            if (!previousBrokersSet.contains(broker)) {
                System.out.println("[INFO]: New broker available: " + broker.getHost() + ":" + broker.getPort());
            }
        }

        for (Address broker : previousBrokers) {
            if (!currentBrokersSet.contains(broker)) {
                System.out.println("[INFO]: Broker unavailable: " + broker.getHost() + ":" + broker.getPort());
            }
        }

        System.out.println("[INFO]: Brokers are updated. Found " + currentBrokers.size() + " brokers.");
        System.out.println("[INFO]: You can connect any of them by specifying the broker index. Starts from 1.");
        System.out.print("[INFO]: Current brokers: " + brokerCache);
        if (showSymbol) {
            System.out.print("\n> ");
        } else {
            System.out.println();
        }
    }

    public void start() {
        while (true) {
            String[] parts = getCommand();
            if (parts.length == 0) {continue;}
            String command = parts[0];
            Message request = new Message();
            boolean shouldSendRequest = false;
            try {
                switch (command) {
                    case Operation.CREATE:
                    case Operation.READ:
                    case Operation.WRITE:
                        if (socket == null || !socket.isConnected()) {
                            System.err.println("[ERROR]: Not connected to a broker");
                            continue;
                        }
                        shouldSendRequest = processOperation(command, parts, request);
                        break;
                    case Operation.CONNECT:
                        if (parts.length != 2) {
                            System.err.println("[ERROR]: connect <broker_index>");
                            continue;
                        }
                        connectToBroker(Integer.parseInt(parts[1]));
                        continue;
                    case Operation.DISCONNECT:
                        if (socket == null || !socket.isConnected()) {
                            System.err.println("[ERROR]: Not connected to any broker");
                            continue;
                        }
                        if (parts.length != 1){
                            System.err.println("[ERROR]: disconnect");
                            continue;
                        }
                        disconnectFromBroker();
                        discoverBrokers(true);
                        continue;
                    default:
                        System.err.println("[ERROR]: Invalid command. Available: create, read, write, connect");
                        continue;
                }
                if (!shouldSendRequest) {
                    continue;
                }

                out.writeObject(request);
                out.flush();
                Message response = (Message) in.readObject();
                if (response.getResponseType().equals(ResponseType.SUCCESS)) {
                    if (response.getClientId() != null && this.id == null) {
                        this.id = response.getClientId();
                        System.out.println("[INFO]: Registered with ID: " + this.id);
                    }

                    if (response.getResponseData() != null) {
                        System.out.println(response.getResponseData());
                    } else {
                        System.out.println(response.getResponseMessage());
                    }
                } else {
                    System.err.println("[ERROR]: " + response.getResponseMessage());
                }
            } catch (Exception e) {
                System.err.println("[ERROR]: Broker connection lost.");
                System.out.println("[INFO]: Re-discovering available brokers.");
                List<Address> previousBrokers = new ArrayList<>(brokerCache);
                if (socket.isConnected() && !socket.isClosed() && !socket.isInputShutdown() && !socket.isOutputShutdown()) {
                    disconnectFromBroker();
                }
                discoverBrokers(false);
                compareAndLogBrokerChanges(previousBrokers, brokerCache,false);
            }
        }
    }

    private boolean processOperation(String command, String[] parts, Message request) {
        request.setClientId(id);
        switch (command) {
            case Operation.CREATE:
                if (parts.length != 2) {
                    System.err.println("[ERROR]: create <queue_name>");
                    return false;
                }
                request.setType(Operation.CREATE);
                request.setQueueName(parts[1]);
                break;
            case Operation.READ:
                if (parts.length != 2) {
                    System.err.println("[ERROR]: read <queue_name>");
                    return false;
                }
                request.setType(Operation.READ);
                request.setQueueName(parts[1]);
                break;
            case Operation.WRITE:
                if (parts.length != 3) {
                    System.err.println("[ERROR]: write <queue_name> <value>");
                    return false;
                }
                request.setType(Operation.WRITE);
                request.setQueueName(parts[1]);
                try {
                    Integer.parseInt(parts[2]);
                } catch (NumberFormatException e) {
                    System.err.println("[ERROR]: Value must be an integer");
                    return false;
                }
                request.setValue(Integer.parseInt(parts[2]));
                break;
        }
        return true;
    }

    private void connectToBroker(int index) {
        try {
            if (socket != null && !socket.isClosed()) {socket.close();}
            Address selectedBroker = getAddressByIndex(index - 1);
            String host = selectedBroker.getHost();
            int port = selectedBroker.getPort();

            System.out.println("[INFO]: Connecting to broker at " + host + ":" + port);
            socket = new Socket(host, port);
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            isConnected.set(true);
            System.out.println("[INFO]: Successfully connected to broker");
        } catch (IOException e) {
            isConnected.set(false);
            System.err.println("[ERROR]: Failed to connect to broker.");
        }
    }

    private Address getAddressByIndex(int index) {
        if (index < 0 || index >= brokerCache.size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + brokerCache.size());
        }

        Iterator<Address> iterator = brokerCache.iterator();
        for (int i = 0; i < index; i++) {
            iterator.next();
        }
        return iterator.next();
    }

    private void disconnectFromBroker() {
        try {
            System.out.println("[INFO]: Disconnecting from broker...");
            socket.close();
            isConnected.set(false);
            System.out.println("[INFO]: Disconnected successfully");
        } catch (IOException e) {
            System.err.println("[ERROR]: While disconnecting.");
        }
    }

    private void discoverBrokers(boolean initialDiscovery) {
        try {
            brokerCache.clear();
            MulticastSocket multicastSocket = new MulticastSocket();
            multicastSocket.setSoTimeout(DISCOVERY_TIMEOUT);
            InetAddress group = InetAddress.getByName(MULTICAST_ADDRESS);

            String message = "DISCOVER_BROKERS";
            byte[] messageBytes = message.getBytes();
            DatagramPacket packet = new DatagramPacket(messageBytes, messageBytes.length, group, MULTICAST_PORT);
            multicastSocket.send(packet);

            while (true) {
                try {
                    byte[] buffer = new byte[1024];
                    DatagramPacket responsePacket = new DatagramPacket(buffer, buffer.length);
                    multicastSocket.receive(responsePacket);
                    Integer brokerHost = Integer.parseInt(new String(responsePacket.getData(), 0, responsePacket.getLength()));
                    String brokerIP = responsePacket.getAddress().getHostAddress();
                    Address newBroker = new Address(brokerIP, brokerHost);

                    if (!brokerCache.contains(newBroker)) {
                        brokerCache.add(newBroker);
                        if (initialDiscovery) {
                            System.out.println("[INFO]: Found broker: " + newBroker.getHost() + ":" + newBroker.getPort());
                        }

                    }
                } catch (SocketTimeoutException e) {
                    if (initialDiscovery) {
                        System.out.println("[INFO]: Initial discovery complete. Found " + brokerCache.size() + " brokers.");
                        System.out.println("[INFO]: Current brokers: " + brokerCache);
                    }
                    break;
                }
            }
            multicastSocket.close();
        } catch (IOException e) {
            System.err.println("[ERROR]: Discovery failed.");
        }
    }

    private String[] getCommand() {
        System.out.print("> ");
        String line = scanner.nextLine().trim();
        String[] parts = line.split(" ");
        return parts;
    }
}