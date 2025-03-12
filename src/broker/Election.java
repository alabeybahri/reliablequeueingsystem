package broker;

import common.Address;
import common.InterBrokerMessage;
import common.Pair;
import common.enums.MessageType;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Election {
    private static final int MIN_ELECTION_TIMEOUT = 2500;
    private static final int MAX_ELECTION_TIMEOUT = 7500;
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

    public void resetElectionTimeout(String queueName){
        int electionTimeout = random.nextInt(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT) + MIN_ELECTION_TIMEOUT;
        ScheduledFuture<?> scheduledFuture;

        // if there is a scheduled timeout for this queue, cancel it
        if (scheduledTimeouts.containsKey(queueName)) {
            scheduledFuture = scheduledTimeouts.get(queueName);
            scheduledFuture.cancel(true);
            scheduledTimeouts.remove(queueName);
        }

        ScheduledFuture<?> newScheduledFuture = schedulerExecutor.schedule(()-> {
            startElection(queueName);
        }, electionTimeout, TimeUnit.MILLISECONDS);
        scheduledTimeouts.put(queueName, newScheduledFuture);

    }


    private void startElection(String queueName) {
        int electionTerm =  broker.terms.get(queueName) + 1;
        int totalVotes = broker.otherFollowers.get(queueName).size() ;
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
        AtomicInteger voteGranted = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(otherFollowers.size());

        System.out.println("[INFO]: [Broker: " + broker.port + "] Starting election for " + queueName + " with term " + electionTerm);

        for (Address address : otherFollowers) {
            executor.submit(() -> {
                try {
                    InterBrokerMessage response = broker.sendElectionMessage(address, queueName, electionTerm);
                    if (response != null && response.getMessageType() == MessageType.VOTE && response.isVote()) {
                        voteGranted.getAndIncrement();
                        }
                } catch (Exception e) {
                    System.err.println("Error processing election request to " + address);
                } finally {
                latch.countDown();
                }
            });
        }

        try {
            boolean allDone = latch.await(5, TimeUnit.SECONDS);
            if (!allDone) {
                System.err.println("Election timed out waiting for votes");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        executor.shutdown();
        int didVotedSelf = votedSelf.get(queueName).contains(electionTerm) ? 1 : 0;

        System.out.println("vote granted count: " + voteGranted.get());
        System.out.println("did voted self: " + didVotedSelf);
        System.out.println("totalVotes: " + totalVotes);

        if (voteGranted.get() + didVotedSelf > (totalVotes + 1) / 2) {
            becomeLeader(queueName, electionTerm, otherFollowers);
        } else {
            System.out.println("[INFO]: [Broker: " + broker.port + "] Could not get enough votes to become leader");
        }

    }

    public void cancelElection(String queueName) {

    }

    public void createElectionTimeout(String queueName, boolean changePool){
        if (changePool) {
            int poolSize = schedulerExecutor.getPoolSize();
            schedulerExecutor.setCorePoolSize(poolSize + 1);
        }
        resetElectionTimeout(queueName);
    }

    private void becomeLeader(String queueName, int newTerm, List<Address> otherFollowers) {
        System.out.println("[INFO]:"+ broker.brokerAddress +" New leader for " + queueName + " with term " + newTerm);
        transferData(queueName, newTerm, otherFollowers);
        broker.startPeriodicPingFollowers(queueName);
    }

    private void transferData(String queueName, int newTerm, List<Address> otherFollowers) {
        broker.terms.put(queueName, newTerm);

        List<Integer> oldReplicatedQueue = new ArrayList<>(broker.replications.get(queueName));
        broker.replications.remove(queueName);
        broker.queues.put(queueName, oldReplicatedQueue);

        Map<String, Integer> oldReplicationClientOffsets = new HashMap<>(broker.replicationClientOffsets.get(queueName));
        broker.replicationClientOffsets.remove(queueName);
        broker.clientOffsets.put(queueName, oldReplicationClientOffsets);

        // add replications to replication brokers...
        otherFollowers.forEach(address ->{
            Pair<Address, Integer> replicationPair = new Pair<>(address, 0);
            broker.replicationBrokers.computeIfAbsent(queueName, k -> new ArrayList<>()).add(replicationPair);
        });
        broker.otherFollowers.remove(queueName);

        broker.terms.put(queueName, newTerm);
    }

}
