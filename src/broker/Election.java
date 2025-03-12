package broker;

import common.Address;
import common.InterBrokerMessage;
import common.enums.MessageType;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Election {
    private static final int MIN_ELECTION_TIMEOUT = 2500;
    private static final int MAX_ELECTION_TIMEOUT = 7500;
    private final Broker broker;
    private final Random random;
    private ScheduledThreadPoolExecutor schedulerExecutor;
    private Map<String, ScheduledFuture<?>> scheduledTimeouts = new ConcurrentHashMap<>();

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
        int totalVotes = broker.otherFollowers.get(queueName).size() ; // including the leader
        ArrayList<Address> otherFollowers = new ArrayList<>(broker.otherFollowers.get(queueName));
        //otherFollowers.add(broker.queueAddressMap.get(queueName));

        if (otherFollowers.isEmpty()) {
            announceLeader();
            return;
        }


        ExecutorService executor = Executors.newFixedThreadPool(otherFollowers.size());
        AtomicInteger voteGranted = new AtomicInteger();
        System.out.println("[INFO]: [Broker: " + broker.port + "] Starting election for " + queueName + " with term " + electionTerm);


        for (Address address : otherFollowers) {
            executor.submit(() -> {
                InterBrokerMessage response = broker.sendElectionMessage(address, queueName, electionTerm);
                if (response != null && response.getMessageType() == MessageType.VOTE) {
                    if (response.isVote()){ voteGranted.getAndIncrement(); }
                }
            });
        }

        if (voteGranted.get() + 1 > totalVotes / 2) {
            announceLeader();
        }
    }


    public void createElectionTimeout(String queueName){
        int poolSize = schedulerExecutor.getPoolSize();
        schedulerExecutor.setCorePoolSize(poolSize + 1);
        resetElectionTimeout(queueName);
    }

    private void announceLeader() {
        System.out.println("[INFO]:"+ broker.brokerAddress +" I am the new leader");
    }

}
