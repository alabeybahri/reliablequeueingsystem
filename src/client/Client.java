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
        startContinuousDiscovery();

        discoverBrokers(true);
        if (brokerCache.isEmpty()) {
            System.out.println("No brokers found.");
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
                    Thread.currentThread().interrupt();
                    break;
                }
            }
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
                System.out.println("New broker joined: " + broker.getHost() + ":" + broker.getPort());
            }
        }

        for (Address broker : previousBrokers) {
            if (!currentBrokersSet.contains(broker)) {
                System.out.println("Broker left: " + broker.getHost() + ":" + broker.getPort());
            }
        }

        System.out.println("Brokers are updated. Found " + currentBrokers.size() + " brokers.");
        System.out.println("You can connect any of them by specifying the broker index. It starts from 1.");
        System.out.print("Current brokers:" + brokerCache);
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
                            System.out.println("Not connected to any broker.");
                            continue;
                        }
                        shouldSendRequest = processOperation(command, parts, request);
                        // Only these operations should send requests
                        break;
                    case Operation.CONNECT:
                        if (parts.length != 2) {
                            System.out.println("Usage: connect <index>");
                            continue;
                        }
                        connectToBroker(Integer.parseInt(parts[1]));
                        continue;
                    case Operation.DISCONNECT:
                        if (socket == null || !socket.isConnected()) {
                            System.out.println("Not connected to any broker.");
                            continue;
                        }
                        if (parts.length != 1){
                            System.out.println("Usage: disconnect");
                            continue;
                        }
                        disconnectFromBroker();
                        discoverBrokers(true);
                        continue;
                    default:
                        System.out.println("Invalid command. Use: create, read, write, connect!");
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
                    }

                    if (response.getResponseData() != null) {
                        System.out.println("Received: " + response.getResponseData());
                    } else {
                        System.out.println(response.getResponseMessage());
                    }
                } else {
                    System.out.println("Error: " + response.getResponseMessage());
                }
            } catch (Exception e) {
                System.out.println("Current broker disconnected, rediscovering other brokers");
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
                    System.out.println("Usage: create <queue_name>");
                    return false;
                }
                request.setType(Operation.CREATE);
                request.setQueueName(parts[1]);
                break;
            case Operation.READ:
                if (parts.length != 2) {
                    System.out.println("Usage: read <queue_name>");
                    return false;
                }
                request.setType(Operation.READ);
                request.setQueueName(parts[1]);
                break;
            case Operation.WRITE:
                if (parts.length != 3) {
                    System.out.println("Usage: write <queue_name> <value>");
                    return false;
                }
                request.setType(Operation.WRITE);
                request.setQueueName(parts[1]);
                try {
                    Integer.parseInt(parts[2]);
                } catch (NumberFormatException e) {
                    System.out.println("Value must be an integer.");
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

            socket = new Socket(host, port);
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            isConnected.set(true);
            System.out.println("Connected to broker at " + host + ":" + port);
        } catch (IOException e) {
            isConnected.set(false);
            e.printStackTrace();
        }
    }

    // Helper method to get Address by index from TreeSet
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
            socket.close();
            isConnected.set(false);
        } catch (IOException e) {
            e.printStackTrace();
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
                            System.out.println("Found new broker: " + newBroker.getHost() + ":" + newBroker.getPort());
                        }
                    }
                } catch (SocketTimeoutException e) {
                    if (initialDiscovery) {
                        System.out.println("Discovery complete. Found " + brokerCache.size() + " brokers.\nYou can connect any of them by specifying the ip and the port or the broker index.");
                        System.out.println("Current brokers:" + brokerCache);
                    }
                    break;
                }
            }
            multicastSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String[] getCommand() {
        System.out.print("> ");
        String line = scanner.nextLine().trim();
        String[] parts = line.split(" ");
        return parts;
    }
}