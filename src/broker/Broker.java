package broker;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import common.*;
import common.enums.MessageType;

public class Broker {
    private int port;
    public Address brokerAddress;
    public Map<String, List<Integer>> queues = new ConcurrentHashMap<>();
    public Map<String, Map<String, Integer>> clientOffsets = new ConcurrentHashMap<>(); // clientId -> (queueName -> offset)
    private MulticastSocket brokerMulticastSocket;
    private InetSocketAddress brokerGroup;
    public List<Address> knownBrokers = new ArrayList<>();
    public Map<String, Address> queueAddressMap = new ConcurrentHashMap<>();
    public Map<String, List<Pair<Address, Integer>>> replicationBrokers = new ConcurrentHashMap<>(); // replication broker addresses of this leader
    public Map<String, Map<String, Integer>> replicationClientOffsets = new ConcurrentHashMap<>(); // clientId -> (queueName -> offset)
    public Map<String, List<Integer>> replications = new ConcurrentHashMap<>(); // replicated queues, leader is another broker
    private static final String CLIENT_MULTICAST_ADDRESS = "239.255.0.2";
    private static final int CLIENT_MULTICAST_PORT = 5010;
    private static final String BROKER_MULTICAST_ADDRESS = "239.255.0.1";
    private static final int BROKER_MULTICAST_PORT = 5020;
    private static final int MIN_FAILURE_TIMEOUT = 100;
    private static final int MAX_FAILURE_TIMEOUT = 200;
    private static final int NUM_ALLOWED_MISSED_PINGS = 3;
    private static final int PING_JITTER = 1000;
    private static final int CONSECUTIVE_FAILURE_LIMIT = 3;
    private int FAILURE_TIMEOUT;
    private int SEND_PING_INTERVAL;
    private int FOLLOWER_PING_INTERVAL = 3000;
    private Random random;
    // Persistent socket pool for broker-broker connections
    private Map<Address, Socket> brokerSocketPool = new ConcurrentHashMap<>();
    private Map<Address, ObjectOutputStream> brokerOutputStreams = new ConcurrentHashMap<>();
    private Map<Address, ObjectInputStream> brokerInputStreams = new ConcurrentHashMap<>();
    private final Object brokerSocketLock = new Object();


    public Broker(int port) throws IOException {
        this.port = port;
        this.brokerAddress = new Address(LocalIP.getLocalIP().toString(),port);
        this.random = new Random(brokerAddress.hashCode());
        FAILURE_TIMEOUT = random.nextInt(MAX_FAILURE_TIMEOUT - MIN_FAILURE_TIMEOUT) + MIN_FAILURE_TIMEOUT;
        SEND_PING_INTERVAL = (int) (FAILURE_TIMEOUT * 0.75 / NUM_ALLOWED_MISSED_PINGS);
        startMulticastListener();
        createBrokerMulticastSocket();
        startBrokerMulticastListener();
        startPeriodicPing();
        initializeConnectionCleanup();
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("[INFO]: [Broker: " + port +  "] Started");
            while (true) {
                Socket socket = serverSocket.accept();
                String clientId = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
                if (knownBrokers.contains(new Address(socket.getInetAddress().getHostAddress(), socket.getPort()))) {
                    System.out.println("[INFO]: [Broker: " + port +  "] Broker connected: " + clientId);
                }else {
                    System.out.println("[INFO]: [Broker: " + port +  "] Client connected: " + clientId);
                }
                new Thread(new ConnectionHandler(socket, clientId, this)).start();

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

        scheduler.scheduleAtFixedRate(() -> {
            try {
                Thread.sleep(random.nextInt(PING_JITTER));
                sendPingRequest();
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }, 0, SEND_PING_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public void startPeriodicPingFollowers(String queueName) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                Thread.sleep(random.nextInt(PING_JITTER));
                createPingToFollowers(queueName);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 0, FOLLOWER_PING_INTERVAL, TimeUnit.MILLISECONDS);

    }


    /**
     * Sends ping messages to its own replication nodes as a heartbeat.
     * @throws IOException
     */
    private void sendPingRequest() throws IOException {
        InterBrokerMessage interBrokerMessage = new InterBrokerMessage();
        interBrokerMessage.setMessageType(MessageType.PING);
        interBrokerMessage.setPort(port);
        byte[] messageBytes = interBrokerMessage.serializeToBytes();
        DatagramPacket datagramPacket = new DatagramPacket(messageBytes, messageBytes.length, brokerGroup.getAddress(), brokerGroup.getPort());
        brokerMulticastSocket.send(datagramPacket);
//        System.out.println("[INFO]: [Broker: " + port +  "] Sent healthcheck to " + BROKER_MULTICAST_ADDRESS + ":" + BROKER_MULTICAST_PORT);
    }


    private int createPingToFollowers(String queueName) {
        ExecutorService executor = Executors.newFixedThreadPool(replicationBrokers.get(queueName).size());
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(replicationBrokers.get(queueName).size());
        for (Pair<Address, Integer>  follower : replicationBrokers.get(queueName)) {
            executor.submit(() -> {
                try {
                    boolean ackReceived = sendPingRequestToFollower(follower.first);
                    if (ackReceived) {
                        successCount.incrementAndGet();
                    } else {
                        follower.second++;
                        if (follower.second >= CONSECUTIVE_FAILURE_LIMIT) {
                            replicationBrokers.get(queueName).remove(follower);
                            System.out.println("[INFO]: Follower failed to respond, removing it from replications :" + follower.first );
                        }
                    }
                } catch (Exception e) {
                    System.err.println("[ERROR]: Ping failed for " + follower + ": " + queueName);
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            boolean allDone = latch.await(10, TimeUnit.SECONDS);
            if (!allDone) {
                System.out.println("[ERROR]: Timeout waiting for ping replies");
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (successCount.get()  == replicationBrokers.get(queueName).size()) {
            System.out.println("[INFO]: [Broker: " + port +  "] Successfully got ACKs from all followers follower count: " + successCount.get() + " from " + replicationBrokers.get(queueName).size() + " followers");
        } else {
            System.out.println("[INFO]: [Broker: " + port +  "] Not received all the ACKs.");
        }
        executor.shutdown();

        return successCount.get();
    }


    private boolean sendPingRequestToFollower(Address follower) {
        try {
            Socket socket = getOrCreateBrokerSocket(follower);
            ObjectOutputStream out = brokerOutputStreams.get(follower);
            ObjectInputStream in = brokerInputStreams.get(follower);

            InterBrokerMessage request = new InterBrokerMessage();
            request.setMessageType(MessageType.PING);
            request.setPort(port);
            out.writeObject(request);
            out.flush();

            socket.setSoTimeout(5000); // 5 seconds for ACK

            InterBrokerMessage response = (InterBrokerMessage) in.readObject();
            System.out.println("[INFO]: [Broker: " + port + "] ACK received for PING message request" + follower);
            return response.getMessageType() == MessageType.ACK;

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[INFO]: [Broker: " + port + "] ACK not received from" + follower);

            // Remove problematic socket from pool
            synchronized (brokerSocketLock) {
                Socket socket = brokerSocketPool.remove(follower);
                try {
                    if (socket != null) socket.close();
                } catch (IOException closeEx) {
                    System.err.println("Error closing socket: " + closeEx.getMessage());
                    return false;
                }
                brokerOutputStreams.remove(follower);
                brokerInputStreams.remove(follower);
                return false;
            }
        }
    }


    /**
     * Sends datagram packet to multicast group to notify newly created queue.
     * @param queueName newly created queue name.
     */
    public void updateQueueAddressMap(String queueName) throws IOException {
        InterBrokerMessage interBrokerMessage = new InterBrokerMessage();
        interBrokerMessage.setQueueName(queueName);
        interBrokerMessage.setLeader(brokerAddress);
        interBrokerMessage.setMessageType(MessageType.NEW_QUEUE);
        byte[] messageBytes = interBrokerMessage.serializeToBytes();
        DatagramPacket packet = new DatagramPacket(messageBytes, messageBytes.length, brokerGroup.getAddress(), BROKER_MULTICAST_PORT);
        brokerMulticastSocket.send(packet);
        System.out.println("[INFO]: [Broker: " + port +  "] Updated queue address map added " + queueName + " leader: " + brokerAddress);
    }

    private void sendPingResponse() throws IOException {
        InterBrokerMessage interBrokerMessage = new InterBrokerMessage();
        interBrokerMessage.setMessageType(MessageType.ACK);
        interBrokerMessage.setPort(port);
        byte[] messageBytes = interBrokerMessage.serializeToBytes();
        DatagramPacket datagramPacket = new DatagramPacket(messageBytes, messageBytes.length, brokerGroup.getAddress(), brokerGroup.getPort());
        brokerMulticastSocket.send(datagramPacket);
//        System.out.println("[INFO]: [Broker: " + port + "] Responded to ping request with " + interBrokerMessage.getPort());
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
                    if (receivedInterBrokerMessage.getMessageType().equals(MessageType.PING) && !((packet.getAddress().equals(LocalIP.getLocalIP())) && (receivedInterBrokerMessage.getPort()) == (port))) {
                        sendPingResponse();
                        registerBroker(new Address(packet.getAddress().getHostAddress(), receivedInterBrokerMessage.getPort()));
                    }
                    else if (receivedInterBrokerMessage.getMessageType().equals(MessageType.NEW_QUEUE)) {
                        queueAddressMap.put(receivedInterBrokerMessage.getQueueName(), receivedInterBrokerMessage.getLeader());
                        sendPingResponse();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    private void registerBroker(Address address) {
        String brokerInfo = address.getHost() + ":" + address.getPort();

//        System.out.println("[INFO]: [Broker: " + this.port +  "] Received response from " + brokerInfo);
        if (!knownBrokers.contains(address)) {
            knownBrokers.add(address);
            System.out.println("[INFO]: [Broker: " + this.port +  "] Registered broker " + brokerInfo);
        }
    }

    /**
     * Selects brokers to replicate its queue. Sends them unicast request to create the queue replication.
     * At this point, it is known that queue is not created before.
     * @param queueName Queue that will be replicated.
     * @return Number of successfully created replicas excluding leader.
     */
    public int createReplication(String queueName){
        int replicationCount = (knownBrokers.size() + 1) / 2;
        if (replicationCount < 1) {
            System.out.println("[ERROR]: Not enough brokers to replicate queue: ");
            return -1;
        }
        List<Address> shuffledBrokers = new ArrayList<>(knownBrokers);
        Collections.shuffle(shuffledBrokers);
        List<Address> selectedBrokers = shuffledBrokers.subList(0, replicationCount);

        ExecutorService executor = Executors.newFixedThreadPool(replicationCount);
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(replicationCount);
        // Send async replication requests
        for (Address broker : selectedBrokers) {
            executor.submit(() -> {
                try {
                    boolean ackReceived = sendReplicationRequest(queueName, broker);
                    if (ackReceived) {
                        successCount.incrementAndGet();
                        registerReplica(queueName, broker);
                    }
                } catch (Exception e) {
                    System.err.println("[ERROR]: Replication failed for " + broker + ": " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            boolean allDone = latch.await(10, TimeUnit.SECONDS); // Overall timeout
            if (!allDone) {
                System.out.println("[ERROR]: Timeout waiting for replications");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        executor.shutdown();
        return successCount.get();
    }


    private boolean sendReplicationRequest(String queueName, Address broker) {
        try {
            Socket socket = getOrCreateBrokerSocket(broker);
            ObjectOutputStream out = brokerOutputStreams.get(broker);
            ObjectInputStream in = brokerInputStreams.get(broker);

            InterBrokerMessage request = new InterBrokerMessage();
            request.setMessageType(MessageType.REPLICATION);
            request.setQueueName(queueName);
            out.writeObject(request);
            out.flush();

            socket.setSoTimeout(5000); // 5 seconds for ACK

            InterBrokerMessage response = (InterBrokerMessage) in.readObject();
            System.out.println("[INFO]: [Broker: " + port + "] ACK received for replication message request, queue:" + request.getQueueName() + " from" + broker);
            return response.getMessageType() == MessageType.ACK;

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[INFO]: [Broker: " + port + "] Replication creation failed for " + broker + ": " + e.getMessage());

            // Remove problematic socket from pool
            synchronized (brokerSocketLock) {
                Socket socket = brokerSocketPool.remove(broker);
                try {
                    if (socket != null) socket.close();
                } catch (IOException closeEx) {
                    System.err.println("Error closing socket: " + closeEx.getMessage());
                }
                brokerOutputStreams.remove(broker);
                brokerInputStreams.remove(broker);
            }

            return false;
        }
    }

    private void registerReplica(String queueName, Address broker) {
        replicationBrokers.computeIfAbsent(queueName, k -> new ArrayList<>())
                .add(new Pair<Address, Integer>(broker, 0));
    }



    public void updateReplications(Message request, String originalClientId) {
        String requestType = request.getType();
        String queueName = request.getQueueName();
        List<Pair<Address, Integer>> brokersToSend = replicationBrokers.get(queueName);
        if (brokersToSend == null) { return; } // there is no replication for this queue

        int replicationCount = brokersToSend.size();
        ExecutorService executor = Executors.newFixedThreadPool(replicationCount);
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(replicationCount);
        for (Pair<Address, Integer> brokerToSend : brokersToSend) {
            executor.submit(() -> {
                try {
                    boolean ackReceived = false;
                    if (requestType.equals(Operation.WRITE)){
                        ackReceived = sendAppendMessageRequestWithClientId(request, brokerToSend.first, originalClientId);
                    } else if (requestType.equals(Operation.READ)){
                        ackReceived = sendReadMessageRequestWithClientId(request, brokerToSend.first, originalClientId);
                    }

                    if (ackReceived) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    System.err.println("[ERROR]: Append/Read replication failed for " + brokerToSend + ": " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            boolean allDone = latch.await(10, TimeUnit.SECONDS); // Overall timeout
            if (!allDone) {
                System.out.println("[ERROR]: Timeout waiting for replications");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        executor.shutdown();
    }

    private boolean sendAppendMessageRequestWithClientId(Message request, Address broker, String originalClientId) {
        try {
            Socket socket = getOrCreateBrokerSocket(broker);
            ObjectOutputStream out = brokerOutputStreams.get(broker);
            ObjectInputStream in = brokerInputStreams.get(broker);

            InterBrokerMessage replicationRequest = new InterBrokerMessage();
            replicationRequest.setMessageType(MessageType.APPEND_MESSAGE);
            replicationRequest.setData(request.getValue());
            replicationRequest.setQueueName(request.getQueueName());
            replicationRequest.setOriginalClientId(originalClientId);
            out.writeObject(replicationRequest);
            out.flush();

            socket.setSoTimeout(5000);

            InterBrokerMessage response = (InterBrokerMessage) in.readObject();
            System.out.println("[INFO]: [Broker: " + port + "] ACK received for append message request, queue:" + replicationRequest.getQueueName() + " from" + broker);
            return response.getMessageType() == MessageType.ACK;

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[ERROR]: Append message to replication failed for " + broker + ": " + e.getMessage());

            // Remove problematic socket from pool
            synchronized (brokerSocketLock) {
                Socket socket = brokerSocketPool.remove(broker);
                try {
                    if (socket != null) socket.close();
                } catch (IOException closeEx) {
                    System.err.println("Error closing socket: " + closeEx.getMessage());
                }
                brokerOutputStreams.remove(broker);
                brokerInputStreams.remove(broker);
            }

            return false;
        }
    }

    private boolean sendReadMessageRequestWithClientId(Message request, Address broker, String originalClientId) {
        try {
            Socket socket = getOrCreateBrokerSocket(broker);
            ObjectOutputStream out = brokerOutputStreams.get(broker);
            ObjectInputStream in = brokerInputStreams.get(broker);

            InterBrokerMessage readRequest = new InterBrokerMessage();
            readRequest.setMessageType(MessageType.READ_MESSAGE);
            readRequest.setQueueName(request.getQueueName());
            readRequest.setOriginalClientId(originalClientId);
            out.writeObject(readRequest);
            out.flush();

            socket.setSoTimeout(5000);

            InterBrokerMessage response = (InterBrokerMessage) in.readObject();
            System.out.println("[INFO]: [Broker: " + port + "] ACK received for read message request, queue:" + readRequest.getQueueName() + " from" + broker);
            return response.getMessageType() == MessageType.ACK;

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[ERROR]: Read message to replication failed for " + broker + ": " + e.getMessage());

            // Remove problematic socket from pool
            synchronized (brokerSocketLock) {
                Socket socket = brokerSocketPool.remove(broker);
                try {
                    if (socket != null) socket.close();
                } catch (IOException closeEx) {
                    System.err.println("Error closing socket: " + closeEx.getMessage());
                }
                brokerOutputStreams.remove(broker);
                brokerInputStreams.remove(broker);
            }

            return false;
        }
    }

    private synchronized Socket getOrCreateBrokerSocket(Address brokerAddress) throws IOException {
        // Check if socket already exists and is valid
        Socket existingSocket = brokerSocketPool.get(brokerAddress);
        if (existingSocket != null && !existingSocket.isClosed() && existingSocket.isConnected()) {
            return existingSocket;
        }

        // Create new socket
        Socket newSocket = new Socket(brokerAddress.getHost(), brokerAddress.getPort());
        ObjectOutputStream out = new ObjectOutputStream(newSocket.getOutputStream());
        ObjectInputStream in = new ObjectInputStream(newSocket.getInputStream());

        // Store socket and streams
        brokerSocketPool.put(brokerAddress, newSocket);
        brokerOutputStreams.put(brokerAddress, out);
        brokerInputStreams.put(brokerAddress, in);

        return newSocket;
    }

    private void initializeConnectionCleanup() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::cleanupBrokerConnections, 5, 5, TimeUnit.MINUTES);
    }

    private void cleanupBrokerConnections() {
        synchronized (brokerSocketLock) {
            Iterator<Map.Entry<Address, Socket>> iterator = brokerSocketPool.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Address, Socket> entry = iterator.next();
                Socket socket = entry.getValue();
                if (socket.isClosed() || !socket.isConnected()) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        System.err.println("Error closing socket: " + e.getMessage());
                    }
                    iterator.remove();
                    brokerOutputStreams.remove(entry.getKey());
                    brokerInputStreams.remove(entry.getKey());
                }
            }
        }
    }
}

