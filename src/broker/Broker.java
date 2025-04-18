package broker;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import common.*;
import common.enums.MessageType;

public class Broker {
    public int port;
    public Address brokerAddress;
    public Map<String, List<Integer>> queues = new ConcurrentHashMap<>();
    public Map<String, Map<String, Integer>> clientOffsets = new ConcurrentHashMap<>(); // clientId -> (queueName -> offset)
    private MulticastSocket brokerMulticastSocket;
    private InetSocketAddress brokerGroup;
    public List<Pair<Address, Integer>> knownBrokers = new ArrayList<>();
    public Map<String, Address> queueAddressMap = new ConcurrentHashMap<>();
    public Map<String, List<Pair<Address, Integer>>> replicationBrokers = new ConcurrentHashMap<>(); // replication broker addresses of this leader
    public Map<String, Map<String, Integer>> replicationClientOffsets = new ConcurrentHashMap<>(); // clientId -> (queueName -> offset)
    public Map<String, List<Integer>> replications = new ConcurrentHashMap<>(); // replicated queues, leader is another broker
    public Map<String, List<Address>> otherFollowers = new ConcurrentHashMap<>();
    public Map<String, HashSet<Integer>> votesGranted = new ConcurrentHashMap<>(); // given votes for terms, if a vote for a term given put term number into the set
    public Map<String, Integer> terms = new ConcurrentHashMap<>();
    private static final String CLIENT_MULTICAST_ADDRESS = "239.255.0.2";
    private static final int CLIENT_MULTICAST_PORT = 5010;
    private static final String BROKER_MULTICAST_ADDRESS = "239.255.0.1";
    private static final int BROKER_MULTICAST_PORT = 5020;
    private static final int MIN_FAILURE_TIMEOUT = 500;
    private static final int MAX_FAILURE_TIMEOUT = 1000;
    private static final int NUM_ALLOWED_MISSED_PINGS = 3;
    private static final int PING_JITTER = 50;
    private static final int CONSECUTIVE_FAILURE_LIMIT = 5;
    private final int FAILURE_TIMEOUT;
    private int SEND_PING_INTERVAL;
    public Random random;
    // Persistent socket pool for broker-broker connections
    private Map<Address, Socket> brokerSocketPool = new ConcurrentHashMap<>();
    private Map<Address, ObjectOutputStream> brokerOutputStreams = new ConcurrentHashMap<>();
    private Map<Address, ObjectInputStream> brokerInputStreams = new ConcurrentHashMap<>();
    private final Object brokerSocketLock = new Object();
    private final Object knownBrokersLock = new Object();
    public final Election electionHandler;
    private final ConcurrentHashMap<String, Object> stateStore = new ConcurrentHashMap<>();

    public Object getStateProperty(String key, Object defaultValue) {
        return stateStore.getOrDefault(key, defaultValue);
    }

    public void setStateProperty(String key, Object value) {
        stateStore.put(key, value);
    }

    public Broker(int port) throws IOException {
        this.port = port;
        this.brokerAddress = new Address(LocalIP.getLocalIP().toString(), port);
        this.random = new Random(brokerAddress.hashCode());
        FAILURE_TIMEOUT = random.nextInt(MAX_FAILURE_TIMEOUT - MIN_FAILURE_TIMEOUT) + MIN_FAILURE_TIMEOUT;
        SEND_PING_INTERVAL = FAILURE_TIMEOUT * NUM_ALLOWED_MISSED_PINGS;
        electionHandler = new Election(this);
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
                new Thread(new ConnectionHandler(socket, clientId, this)).start();

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void removeClient(String clientId) {
        clientOffsets.remove(clientId);
        System.out.println("[INFO]: [Broker:" + port+ "] Disconnected. : " + clientId);
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
        }, 0, SEND_PING_INTERVAL / 3, TimeUnit.MILLISECONDS);

    }


    /**
     * Sends ping messages to its own replication nodes as a heartbeat.
     * @throws IOException
     */
    private void sendPingRequest() throws IOException {
        InterBrokerMessage interBrokerMessage = new InterBrokerMessage();
        interBrokerMessage.setMessageType(MessageType.DISCOVER);
        interBrokerMessage.setPort(port);
        byte[] messageBytes = interBrokerMessage.serializeToBytes();
        DatagramPacket datagramPacket = new DatagramPacket(messageBytes, messageBytes.length, brokerGroup.getAddress(), brokerGroup.getPort());
        brokerMulticastSocket.send(datagramPacket);
        incrementMissedACKsAndCleanup();
    }


    private void createPingToFollowers(String queueName){
        ExecutorService executor = Executors.newFixedThreadPool(replicationBrokers.get(queueName).size());
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(replicationBrokers.get(queueName).size());
        for (Pair<Address, Integer>  follower : replicationBrokers.get(queueName)) {
            executor.submit(() -> {
                try {
                    boolean ackReceived = sendPingRequestToFollower(follower.first, queueName);
                    if (ackReceived) {
                        successCount.incrementAndGet();
                        follower.second = 0;
                    } else {
                        follower.second++;
                        if (follower.second >= CONSECUTIVE_FAILURE_LIMIT) {
                            replicationBrokers.get(queueName).remove(follower);
                            System.out.println("[INFO]: [Broker: " + port+ "] Follower failed to respond, removing it from replications :" + follower.first );
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
                System.err.println("[ERROR]: Timeout waiting for ping replies");
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (successCount.get()  == replicationBrokers.get(queueName).size() && !replicationBrokers.get(queueName).isEmpty()) {
            System.out.println("[INFO]: [Broker: " + port + "] Successfully got ACKs from all followers. " + successCount.get() + "/" + replicationBrokers.get(queueName).size() + " For queue: " + queueName);
        } else if (!replicationBrokers.get(queueName).isEmpty()) {
            System.err.println("[INFO]: [Broker: " + port + "] Not received all the ACKs for the queue: " + queueName + " Received: " + successCount.get() + "/" + replicationBrokers.get(queueName).size());
        }
        executor.shutdown();

        successCount.get();
    }


    private boolean sendPingRequestToFollower(Address follower, String queueName){
        try {
            Socket socket = getOrCreateBrokerSocket(follower);
            ObjectOutputStream out = brokerOutputStreams.get(follower);
            ObjectInputStream in = brokerInputStreams.get(follower);

            InterBrokerMessage request = new InterBrokerMessage();
            request.setMessageType(MessageType.PING);
            request.setQueueName(queueName);
            request.setTerm(terms.get(queueName));
            request.setPort(port);
            request.setLeader(this.brokerAddress);
            List<Address> followers = new ArrayList();
            replicationBrokers.get(queueName).forEach(p ->
                followers.add(p.first));
            request.setFollowerAddresses(followers);
            out.writeObject(request);
            out.flush();

            socket.setSoTimeout(5000);

            InterBrokerMessage response = (InterBrokerMessage) in.readObject();
            return response.getMessageType() == MessageType.ACK;

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[INFO]: [Broker: " + port + "] ACK not received from follower: " + follower);

            // Remove problematic socket from pool
            synchronized (brokerSocketLock) {
                Socket socket = brokerSocketPool.remove(follower);
                try {
                    if (socket != null) socket.close();
                } catch (IOException closeEx) {
                    System.err.println("[ERROR]: Error while closing socket: " + closeEx.getMessage());
                    return false;
                }
                brokerOutputStreams.remove(follower);
                brokerInputStreams.remove(follower);
                return false;
            }
        }
    }


    public void updateQueueAddressMap(String queueName, MessageType messageType, int term) throws IOException {
        InterBrokerMessage interBrokerMessage = new InterBrokerMessage();
        interBrokerMessage.setQueueName(queueName);
        interBrokerMessage.setLeader(brokerAddress);
        interBrokerMessage.setPort(port);
        interBrokerMessage.setMessageType(messageType);
        interBrokerMessage.setTerm(term);
        byte[] messageBytes = interBrokerMessage.serializeToBytes();
        DatagramPacket packet = new DatagramPacket(messageBytes, messageBytes.length, brokerGroup.getAddress(), BROKER_MULTICAST_PORT);
        brokerMulticastSocket.send(packet);
        System.out.println("[INFO]: [Broker: " + port +  "] Now the leader of the queue: " + queueName + " with term: " + term);
    }

    private void sendPingResponse() throws IOException {
        InterBrokerMessage interBrokerMessage = new InterBrokerMessage();
        interBrokerMessage.setMessageType(MessageType.ACK);
        interBrokerMessage.setPort(port);
        byte[] messageBytes = interBrokerMessage.serializeToBytes();
        DatagramPacket datagramPacket = new DatagramPacket(messageBytes, messageBytes.length, brokerGroup.getAddress(), brokerGroup.getPort());
        brokerMulticastSocket.send(datagramPacket);
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
                    byte[] buffer = new byte[4096];
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
                    byte[] buffer = new byte[4096];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    brokerMulticastSocket.receive(packet);
                    InterBrokerMessage receivedInterBrokerMessage = InterBrokerMessage.deserializeFromBytes(packet.getData());
                    if (receivedInterBrokerMessage.getMessageType().equals(MessageType.DISCOVER) && !((packet.getAddress().equals(LocalIP.getLocalIP())) && (receivedInterBrokerMessage.getPort()) == (port))) {
                        sendPingResponse();
                        registerBroker(new Address(packet.getAddress().getHostAddress(), receivedInterBrokerMessage.getPort()));
                    }
                    else if (receivedInterBrokerMessage.getMessageType().equals(MessageType.NEW_QUEUE)) {
                        queueAddressMap.put(receivedInterBrokerMessage.getQueueName(), receivedInterBrokerMessage.getLeader());
                        sendPingResponse();
                    }
                    else if (receivedInterBrokerMessage.getMessageType().equals(MessageType.ACK) && !((packet.getAddress().equals(LocalIP.getLocalIP())) && (receivedInterBrokerMessage.getPort()) == (port))) {
                        resetMissedACKs(new Address(packet.getAddress().getHostAddress(), receivedInterBrokerMessage.getPort()));
                    }
                    else if(receivedInterBrokerMessage.getMessageType().equals(MessageType.LEADER_ANNOUNCEMENT)) {
                        String queueName = receivedInterBrokerMessage.getQueueName();
                        int receivedTerm = receivedInterBrokerMessage.getTerm();
                        Address newLeader = receivedInterBrokerMessage.getLeader();

                        if (!terms.containsKey(queueName) || receivedTerm > terms.get(queueName)) {
                            System.out.println("[INFO]: [Broker: " + port + "] Accepting new leader for queue " + queueName + " with term " + receivedTerm + " at address " + newLeader);
                            queueAddressMap.put(queueName, newLeader);
                            terms.put(queueName, receivedTerm);

                            electionHandler.cancelElectionTimeout(queueName);

                            if (!newLeader.equals(brokerAddress)) {
                                electionHandler.resetElectionTimeout(queueName);

                            }
                        }
                        else if (receivedTerm == terms.get(queueName) &&
                                !newLeader.equals(queueAddressMap.getOrDefault(queueName, null)) &&
                                !(packet.getAddress().equals(LocalIP.getLocalIP()) &&
                                        receivedInterBrokerMessage.getPort() == port)) {

                            if (newLeader.getPort() > brokerAddress.getPort()) {
                                System.out.println("[INFO]: [Broker: " + port + "] Split vote detected for queue " + queueName + ". Yielding to higher port broker.");
                                queueAddressMap.put(queueName, newLeader);
                                terms.put(queueName, receivedTerm);
                                electionHandler.cancelElectionTimeout(queueName);
                                electionHandler.resetElectionTimeout(queueName);
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }



    private void registerBroker(Address address) {
        for (Pair<Address, Integer> broker : knownBrokers) {
            if (broker.first.equals(address)) {
                return;
            }
        }
        knownBrokers.add(new Pair<>(address, 0));
        System.out.println("[INFO]: [Broker: " + port +  "] Registered new broker: " + address.getHost() + ":" + address.getPort());
    }


    public int createReplication(String queueName){
        List<Pair<Address, Integer>> shuffledBrokers;
        int replicationCount;
        synchronized(knownBrokersLock) {
            replicationCount = (knownBrokers.size() + 1) / 2;
            if (replicationCount < 1) {
                System.out.println("[INFO]: Not enough brokers to replicate queue: " + queueName);
                return -1;
            }
             shuffledBrokers = new ArrayList<>(knownBrokers);
        }

        Collections.shuffle(shuffledBrokers);
        ArrayList<Pair<Address, Integer>> selectedBrokersPairs = new ArrayList<>(shuffledBrokers.subList(0, replicationCount));
        List<Address> selectedBrokers = new ArrayList<>();
        for (Pair<Address, Integer> pair : selectedBrokersPairs) {
            selectedBrokers.add(pair.first);
        }
        ExecutorService executor = Executors.newFixedThreadPool(replicationCount);
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(replicationCount);
        // Send async replication requests
        for (Address broker : selectedBrokers) {
            executor.submit(() -> {
                try {
                    boolean ackReceived = sendReplicationRequest(queueName, broker, selectedBrokers);
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
                System.err.println("[ERROR]: Timeout waiting for replications");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        executor.shutdown();
        return successCount.get();
    }


    private boolean sendReplicationRequest(String queueName, Address broker, List<Address> allFollowers) {
        try {
            Socket socket = getOrCreateBrokerSocket(broker);
            ObjectOutputStream out = brokerOutputStreams.get(broker);
            ObjectInputStream in = brokerInputStreams.get(broker);

            // Create a copy of the followers list for this request
            List<Address> followersForThisRequest = new ArrayList<>(allFollowers);
            // Remove the current broker from the copied list
            followersForThisRequest.remove(broker);

            InterBrokerMessage request = new InterBrokerMessage();
            request.setMessageType(MessageType.REPLICATION);
            request.setQueueName(queueName);
            request.setFollowerAddresses(followersForThisRequest);
            out.writeObject(request);
            out.flush();

            socket.setSoTimeout(5000); // 5 seconds for ACK

            InterBrokerMessage response = (InterBrokerMessage) in.readObject();
            System.out.println("[INFO]: [Broker: " + port + "] ACK received for replication message request, queue:" + request.getQueueName() + " from " + broker);
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
        if (brokersToSend == null || brokersToSend.isEmpty()) { return; } // there is no replication for this queue

        int replicationCount = brokersToSend.size();
        ExecutorService executor = Executors.newFixedThreadPool(replicationCount);
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(replicationCount);

        for (Pair<Address, Integer> brokerToSend : brokersToSend) {
            executor.submit(() -> {
                try {
                    boolean ackReceived = false;
                    int maxRetries = 3;
                    int attempts = 0;

                    while (!ackReceived && attempts < maxRetries) {
                        attempts++;
                        try {
                            if (requestType.equals(Operation.WRITE)) {
                                ackReceived = sendAppendMessageRequestWithClientId(request, brokerToSend.first, originalClientId);
                            } else if (requestType.equals(Operation.READ)) {
                                ackReceived = sendReadMessageRequestWithClientId(request, brokerToSend.first, originalClientId);
                            }

                            if (ackReceived) {
                                successCount.incrementAndGet();
                                break;
                            } else if (attempts < maxRetries) {
                                System.err.println("[INFO]: Replication attempt " + attempts + " failed for " + brokerToSend.first + ", retrying in " + (attempts * 500) + "ms...");
                                Thread.sleep(attempts * 500);
                            }
                        } catch (Exception e) {
                            if (attempts < maxRetries) {
                                System.err.println("[ERROR]: Replication attempt " + attempts + " failed for " + brokerToSend + ": " + e.getMessage() + ", retrying...");
                                Thread.sleep(attempts * 500);
                            } else {
                                System.err.println("[ERROR]: All replication attempts failed for " + brokerToSend + ": " + e.getMessage());
                            }
                        }
                    }

                    if (!ackReceived) {
                        System.err.println("[ERROR]: Failed to replicate after " + maxRetries + " attempts to " + brokerToSend.first);
                    }
                } catch (Exception e) {
                    System.err.println("[ERROR]: Unexpected error in replication thread for " + brokerToSend + ": " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            boolean allDone = latch.await(10, TimeUnit.SECONDS);
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
            System.out.println("[INFO]: [Broker: " + port + "] ACK received for append message request, queue:" + replicationRequest.getQueueName() + " from " + broker);
            return response.getMessageType() == MessageType.ACK;

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[ERROR]: Append message to replication failed for " + broker + ": " + e.getMessage());

            // Remove problematic socket from pool
            synchronized (brokerSocketLock) {
                Socket socket = brokerSocketPool.remove(broker);
                try {
                    if (socket != null) socket.close();
                } catch (IOException closeEx) {
                    System.err.println("[ERROR]: Error when closing socket: " + closeEx.getMessage());
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
            if (response.getMessageType() == MessageType.ACK) {
                System.out.println("[INFO]: [Broker: " + port + "] ACK received for read message request, queue:" + readRequest.getQueueName() + " from " + broker);
            }
            return response.getMessageType() == MessageType.ACK;

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[ERROR]: Read message to replication failed for " + broker );

            // Remove problematic socket from pool
            synchronized (brokerSocketLock) {
                Socket socket = brokerSocketPool.remove(broker);
                try {
                    if (socket != null) socket.close();
                } catch (IOException closeEx) {
                    System.err.println("[ERROR]: closing socket: " + closeEx.getMessage());
                }
                brokerOutputStreams.remove(broker);
                brokerInputStreams.remove(broker);
            }

            return false;
        }
    }

    public InterBrokerMessage sendElectionMessage(Address broker, String queueName, int electionTerm){
        try {
            Socket socket = getOrCreateBrokerSocket(broker);
            ObjectOutputStream out = brokerOutputStreams.get(broker);
            InterBrokerMessage electionRequest = new InterBrokerMessage();
            votesGranted.computeIfAbsent(queueName, k -> new HashSet<>()).add(electionTerm);
            electionRequest.setQueueName(queueName);
            electionRequest.setTerm(electionTerm);
            electionRequest.setMessageType(MessageType.ELECTION);
            out.writeObject(electionRequest);
            out.flush();
            socket.setSoTimeout(5000);

            ObjectInputStream in = brokerInputStreams.get(broker);
            return (InterBrokerMessage) in.readObject();

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[ERROR]: Election message failed to " + broker);
        }
        return null;
    }

    public InterBrokerMessage forwardClientReadMessage(Address broker, String queueName, String originalClientId){
        try {
            Socket socket = getOrCreateBrokerSocket(broker);
            ObjectOutputStream out = brokerOutputStreams.get(broker);
            ObjectInputStream in = brokerInputStreams.get(broker);

            InterBrokerMessage readRequest = new InterBrokerMessage();
            readRequest.setMessageType(MessageType.BROKER_READ);
            readRequest.setQueueName(queueName);
            readRequest.setOriginalClientId(originalClientId);
            out.writeObject(readRequest);
            out.flush();

            socket.setSoTimeout(5000);

            InterBrokerMessage response = (InterBrokerMessage) in.readObject();
            if (response.getMessageType() == MessageType.ACK) {
                System.out.println("[INFO]: [Broker: " + port + "] Forwarded read message request, queue:" + readRequest.getQueueName() + " to leader: " + broker);
            }


            return response;

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[ERROR]: Forward read message to leader failed. " + broker );

            // Remove problematic socket from pool
            synchronized (brokerSocketLock) {
                Socket socket = brokerSocketPool.remove(broker);
                try {
                    if (socket != null) socket.close();
                } catch (IOException closeEx) {
                    System.err.println("[ERROR]: closing socket: " + closeEx.getMessage());
                }
                brokerOutputStreams.remove(broker);
                brokerInputStreams.remove(broker);
            }

            return null;
        }

    }

    public InterBrokerMessage forwardClientWriteMessage(Address broker, String queueName, Message request) {
        try {
            Socket socket = getOrCreateBrokerSocket(broker);
            ObjectOutputStream out = brokerOutputStreams.get(broker);
            ObjectInputStream in = brokerInputStreams.get(broker);

            InterBrokerMessage writeRequest = new InterBrokerMessage();
            writeRequest.setMessageType(MessageType.BROKER_WRITE);
            writeRequest.setQueueName(queueName);
            writeRequest.setOriginalClientId(request.getClientId());
            writeRequest.setData(request.getValue());

            out.writeObject(writeRequest);
            out.flush();

            socket.setSoTimeout(5000);

            InterBrokerMessage response = (InterBrokerMessage) in.readObject();
            if (response.getMessageType() == MessageType.ACK) {
                System.out.println("[INFO]: [Broker: " + port + "] ACK received for forwarding write message request, queue:" + writeRequest.getQueueName() + " from " + broker);
            }


            return response;

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[ERROR]: Forward write message to leader failed for " + broker );

            // Remove problematic socket from pool
            synchronized (brokerSocketLock) {
                Socket socket = brokerSocketPool.remove(broker);
                try {
                    if (socket != null) socket.close();
                } catch (IOException closeEx) {
                    System.err.println("[ERROR]: closing socket: " + closeEx.getMessage());
                }
                brokerOutputStreams.remove(broker);
                brokerInputStreams.remove(broker);
            }

            return null;
        }

    }

    public synchronized Socket getOrCreateBrokerSocket(Address brokerAddress) throws IOException {
        Socket existingSocket = brokerSocketPool.get(brokerAddress);
        if (existingSocket != null && !existingSocket.isClosed() && existingSocket.isConnected()) {
            return existingSocket;
        }

        Socket newSocket = new Socket(brokerAddress.getHost(), brokerAddress.getPort());
        ObjectOutputStream out = new ObjectOutputStream(newSocket.getOutputStream());
        ObjectInputStream in = new ObjectInputStream(newSocket.getInputStream());

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
                        System.err.println("[ERROR]: Error while closing socket: " + e.getMessage());
                    }
                    iterator.remove();
                    brokerOutputStreams.remove(entry.getKey());
                    brokerInputStreams.remove(entry.getKey());
                }
            }
        }
    }

    private void resetMissedACKs(Address address) {

        synchronized (knownBrokersLock) {
            for (int i = 0; i < knownBrokers.size(); i++) {
                Pair<Address, Integer> broker = knownBrokers.get(i);
                if (broker.first.equals(address)) {
                    knownBrokers.set(i, new Pair<>(address, 0));
                    break;
                }
            }
        }
    }

    private void incrementMissedACKsAndCleanup() {
        List<Pair<Address, Integer>> updatedBrokers = new ArrayList<>();
        List<Address> brokersToRemove = new ArrayList<>();

        synchronized (knownBrokersLock) {
            for (Pair<Address, Integer> broker : knownBrokers) {
                Address address = broker.first;
                int missedACKs = broker.second + 1;

                if (missedACKs >= CONSECUTIVE_FAILURE_LIMIT) {
                    brokersToRemove.add(address);
                    System.out.println("[INFO]: [Broker: " + port + "] Removing unresponsive broker " + address.getHost() + ":" + address.getPort() + " after 3 missed ACKs");
                } else {
                    updatedBrokers.add(new Pair<>(address, missedACKs));
                }
            }
            knownBrokers = updatedBrokers;
        }
    }



}

