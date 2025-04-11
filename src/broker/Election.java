package broker;

import common.Address;
import common.InterBrokerMessage;
import common.Pair;
import common.enums.MessageType;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Election {
    private static final int MIN_ELECTION_TIMEOUT = 10000;
    private static final int MAX_ELECTION_TIMEOUT = 20000;
    private final Broker broker;
    private final Random random;
    private ScheduledThreadPoolExecutor schedulerExecutor;
    private Map<String, ScheduledFuture<?>> scheduledTimeouts = new ConcurrentHashMap<>();
    private Map<String, HashSet<Integer>> votedSelf = new ConcurrentHashMap<>();
    public Election(Broker broker) {
        this.broker = broker;
        this.random = broker.random;
        this.schedulerExecutor = new ScheduledThreadPoolExecutor(2);
    }

    public void resetElectionTimeout(String queueName) {
        if (broker.queueAddressMap.containsKey(queueName) &&
                broker.queueAddressMap.get(queueName) != null &&
                broker.queueAddressMap.get(queueName).equals(broker.brokerAddress)) {
            cancelElectionTimeout(queueName);
            return;
        }

        int electionTimeout = random.nextInt(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT) + MIN_ELECTION_TIMEOUT;

        cancelElectionTimeout(queueName);

        ScheduledFuture<?> newScheduledFuture = schedulerExecutor.schedule(() -> {
            try {
                if (!broker.brokerAddress.equals(broker.queueAddressMap.getOrDefault(queueName, null))) {
                    startElection(queueName);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, electionTimeout, TimeUnit.MILLISECONDS);

        scheduledTimeouts.put(queueName, newScheduledFuture);
//        System.out.println("[INFO]: [Broker: " + broker.port + "] Set election timeout for " +
//                queueName + " to " + electionTimeout + "ms");
    }

    public void cancelElectionTimeout(String queueName){
        ScheduledFuture<?> scheduledFuture;

        if (scheduledTimeouts.containsKey(queueName)) {
            scheduledFuture = scheduledTimeouts.get(queueName);
            scheduledFuture.cancel(true);
        }
    }

    private void startElection(String queueName) throws IOException {
        if (broker.queueAddressMap.containsKey(queueName) &&
                broker.queueAddressMap.get(queueName) != null &&
                broker.queueAddressMap.get(queueName).equals(broker.brokerAddress)) {
            System.out.println("[INFO]: [Broker: " + broker.port + "] Already the leader for " +
                    queueName + ", skipping election");
            return;
        }

        String electionKey = queueName + "-lastElection";
        Long lastElectionTime = (Long) broker.getStateProperty(electionKey, 0L);
        long currentTime = System.currentTimeMillis();

        if (currentTime - lastElectionTime < MIN_ELECTION_TIMEOUT) {
            System.out.println("[INFO]: [Broker: " + broker.port + "] Election for " + queueName +
                    " attempted too soon, delaying");
            resetElectionTimeout(queueName);
            return;
        }

        broker.setStateProperty(electionKey, currentTime);

        int electionTerm =  broker.terms.get(queueName) + 1;
        int totalVotes = broker.otherFollowers.get(queueName).size();
        ArrayList<Address> otherFollowers = new ArrayList<>(broker.otherFollowers.get(queueName));

        if (otherFollowers.isEmpty()) {
            becomeLeader(queueName, electionTerm, otherFollowers);
            return;
        }

        broker.votesGranted.computeIfAbsent(queueName, k -> new HashSet<>());
        votedSelf.computeIfAbsent(queueName, k -> new HashSet<>());

        if(!broker.votesGranted.get(queueName).contains(electionTerm)) {
            broker.votesGranted.get(queueName).add(electionTerm);
            votedSelf.get(queueName).add(electionTerm);
        }

        ExecutorService executor = Executors.newFixedThreadPool(otherFollowers.size());
        AtomicInteger voteReceived = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(otherFollowers.size());

        System.out.println("[INFO]: [Broker: " + broker.port + "] Starting election for " + queueName + " with term " + electionTerm);

        for (Address address : otherFollowers) {
            executor.submit(() -> {
                try {
                    InterBrokerMessage response = broker.sendElectionMessage(address, queueName, electionTerm);
                    if (response != null && response.getMessageType() == MessageType.VOTE && response.isVote()) {
                        voteReceived.getAndIncrement();
                    }
                } catch (Exception e) {
                    System.err.println("[ERROR]: [Broker:"  + broker.port + "] Error processing election request to " + address);
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            boolean allDone = latch.await(5, TimeUnit.SECONDS);
            if (!allDone) {
                System.err.println("[INFO]: [Broker:"  + broker.port + "] Election timed out waiting for votes");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        executor.shutdown();
        int didVotedSelf = votedSelf.get(queueName).contains(electionTerm) ? 1 : 0;

        System.out.println("[INFO]: [Broker: " + broker.port + "] Vote granted count: " + voteReceived.get());
        System.out.println("[INFO]: [Broker: " + broker.port + "] Did voted self: " + didVotedSelf);
        System.out.println("[INFO]: [Broker: " + broker.port + "] Total votes: " + totalVotes);

        if (voteReceived.get() + didVotedSelf > (totalVotes + 1) / 2) {
            becomeLeader(queueName, electionTerm, otherFollowers);
        } else {
            System.out.println("[INFO]: [Broker: " + broker.port + "] Could not get enough votes to become leader");
        }
    }

    public void createElectionTimeout(String queueName){
        int poolSize = schedulerExecutor.getPoolSize();
        schedulerExecutor.setCorePoolSize(poolSize + 1);
        resetElectionTimeout(queueName);
    }

    private void becomeLeader(String queueName, int newTerm, List<Address> otherFollowers) throws IOException {
        System.out.println("[INFO]: [Broker: " + broker.port +  "] New leader for " + queueName + " with term " + newTerm);
        cancelElectionTimeout(queueName);
        broker.queueAddressMap.replace(queueName, this.broker.brokerAddress);
        broker.updateQueueAddressMap(queueName, MessageType.LEADER_ANNOUNCEMENT, newTerm);
        transferData(queueName, newTerm, otherFollowers);
        broker.startPeriodicPingFollowers(queueName);
    }

    private void transferData(String queueName, int newTerm, List<Address> otherFollowers) {
        broker.terms.put(queueName, newTerm);

        List<Integer> oldReplicatedQueue = new ArrayList<>(broker.replications.get(queueName));
        broker.replications.remove(queueName);
        broker.queues.put(queueName, oldReplicatedQueue);

        synchronized (broker.replicationClientOffsets) {
            Map<String, Integer> oldReplicationClientOffsets =
                    new HashMap<>(broker.replicationClientOffsets.get(queueName));
            broker.replicationClientOffsets.remove(queueName);
            broker.clientOffsets.put(queueName, oldReplicationClientOffsets);
        }

        otherFollowers.forEach(address ->{
            Pair<Address, Integer> replicationPair = new Pair<>(address, 0);
            broker.replicationBrokers.computeIfAbsent(queueName, k -> new ArrayList<>()).add(replicationPair);
        });

        broker.terms.put(queueName, newTerm);
    }
}