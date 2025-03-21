package broker;

import common.Address;
import common.InterBrokerMessage;
import common.Message;
import common.Operation;
import common.enums.MessageType;
import common.enums.ResponseType;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.*;

public class ConnectionHandler implements Runnable {
    private Socket socket;
    private String connectionAddress;
    private Broker broker;
    private ObjectInputStream in;
    private ObjectOutputStream out;

    public ConnectionHandler(Socket socket, String connectionAddress, Broker broker) {
        this.socket = socket;
        this.connectionAddress = connectionAddress;
        this.broker = broker;
        try {
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
        } catch (IOException e) {
            System.out.println("Connection error");
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                Object request = in.readObject();
                if (request == null) break;
                if (request instanceof InterBrokerMessage){
                    InterBrokerMessage responseToBroker = processBrokerRequest((InterBrokerMessage) request);
                    out.writeObject(responseToBroker);
                    out.flush();

                } else if (request instanceof Message){
                    Message responseToClient = processClientRequest((Message) request);
                    out.writeObject(responseToClient);
                    out.flush();
                }
            }
        } catch (EOFException e) {
            // Client disconnected
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            broker.removeClient(connectionAddress);
            closeResources();
        }
    }

    private void closeResources() {
        try {
            if (in != null) in.close();
            if (out != null) out.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            System.out.println("Error closing resources");
        }
    }

    private void handleQueueCreate(Message request, Message response) throws IOException {
        if (this.broker.queueAddressMap.get(request.getQueueName()) != null) {
            response.setResponseType(ResponseType.ERROR);
            response.setResponseMessage("There is already a queue with this name");
        } else {
            broker.queues.put(request.getQueueName(), new ArrayList<>());
            this.broker.queueAddressMap.put(request.getQueueName(), this.broker.brokerAddress);
            broker.createReplication(request.getQueueName());
            response.setResponseType(ResponseType.SUCCESS);
            response.setResponseMessage("Successfully added a queue with this name");
            broker.terms.put(request.getQueueName(), 1);
            broker.updateQueueAddressMap(request.getQueueName());
            broker.startPeriodicPingFollowers(request.getQueueName());
        }
    }

    private void handleQueueWrite(Message request, Message response) {
        if (this.broker.queues.get(request.getQueueName()) == null) {
            Address leaderAddress = this.broker.queueAddressMap.get(request.getQueueName());
            if (leaderAddress == null) {
                response.setResponseType(ResponseType.ERROR);
                response.setResponseMessage("There is no queue");
            }
            else {
                // if there is replication, send message to replications
                broker.updateReplications(request, request.getClientId());
                response.setResponseMessage("This broker is not the leader of this queue, leader is: " + leaderAddress);
            }
        }
        else {
            broker.queues.get(request.getQueueName()).add(request.getValue());
            broker.updateReplications(request, request.getClientId());
            response.setResponseType(ResponseType.SUCCESS);
            response.setResponseMessage("Written successfully");
        }
    }

    private void handleQueueRead(Message request, Message response) {
        if (this.broker.queues.get(request.getQueueName()) == null) {
            String leaderAddress = this.broker.queueAddressMap.get(request.getQueueName()).toString();
            if (leaderAddress == null) {
                response.setResponseType(ResponseType.ERROR);
                response.setResponseMessage("There is no queue");
            }
            else {
                response.setResponseType(ResponseType.SUCCESS);
                response.setResponseMessage("This broker is not the leader of this queue, leader is: " + leaderAddress);
            }
        }
        else {
            List<Integer> queue = broker.queues.get(request.getQueueName());
            Map<String, Integer> offsets = broker.clientOffsets.computeIfAbsent(request.getQueueName(), k -> new HashMap<>());
            int offset = offsets.getOrDefault(request.getClientId(), 0);
            if (offset < queue.size()) {
                Integer value = queue.get(offset);
                offsets.put(request.getClientId(), offset + 1);
                broker.updateReplications(request, request.getClientId());
                response.setResponseType(ResponseType.SUCCESS);
                response.setResponseData(value);
            } else {
                response.setResponseType(ResponseType.ERROR);
                response.setResponseMessage("No more messages");
            }
        }
    }

    private Message processClientRequest(Message request) throws IOException {
        String type = request.getType();
        Message response = new Message();

        if (request.getClientId() == null){
            String newClientId = UUID.randomUUID().toString();
            request.setClientId(newClientId);
        }

        switch (type) {
            case Operation.CREATE:
                synchronized (broker.queues) {
                    handleQueueCreate(request, response);
                }
                break;
            case Operation.WRITE:
                synchronized (broker.queues) {
                    handleQueueWrite(request, response);
                }
                break;
            case Operation.READ:
                synchronized (broker.queues) {
                    handleQueueRead(request, response);
                }
                break;
            default:
                response.setResponseType(ResponseType.ERROR);
                response.setResponseMessage("Invalid request type");
        }

        response.setClientId(request.getClientId());
        return response;
    }


    private InterBrokerMessage processBrokerRequest(InterBrokerMessage request) throws IOException {
        MessageType messageType = request.getMessageType();
        InterBrokerMessage response = new InterBrokerMessage();

        switch (messageType) {
            case REPLICATION:
                handleReplicationRequest(request, response);
                break;
            case APPEND_MESSAGE:
                handleAppendMessage(request, response);
                break;
            case READ_MESSAGE:
                handleReadMessage(request, response);
                break;
            case ACK:
                break;
            case PING:
                handlePingMessage(request, response);
                break;
            case ELECTION:
                handleElectionMessage(request, response);
                break;
            default:
                response.setMessageType(MessageType.NACK);
                break;
        }
        return response;
    }

    private void handleElectionMessage(InterBrokerMessage request, InterBrokerMessage response) {
        String queueName = request.getQueueName();
        int voteTerm = request.getTerm();
        int currentTerm = broker.terms.get(queueName);
        response.setMessageType(MessageType.VOTE);
        if (!broker.votesGranted.containsKey(queueName)) {
            broker.votesGranted.put(queueName, new HashSet<>());
        }
        boolean vote = voteTerm > currentTerm;
        // if given term, the broker already positive vote do not send positive vote for that term.
        vote = vote && !broker.votesGranted.get(queueName).contains(voteTerm);

        if (vote) {
            broker.votesGranted.get(queueName).add(voteTerm);
        }

        response.setVote(vote);
        System.out.println("[INFO]: [Broker:" + broker.port + "] Voting : " + vote + ", Vote term: " + voteTerm + " current term: " + currentTerm);
    }

    private void handleReplicationRequest(InterBrokerMessage request, InterBrokerMessage response) {
        String queueName = request.getQueueName();

        // Check if queue already exists
        if (this.broker.queues.get(queueName) != null) {
            System.out.println("Leader of this queue, should not take this replication request: " + queueName);
            response.setMessageType(MessageType.NACK);
            return;
        }

        broker.replications.put(queueName, new ArrayList<>());
        broker.terms.put(queueName, 1);
        Map<String, Integer> replicationOffset = new HashMap<>();
        broker.replicationClientOffsets.put(queueName, replicationOffset);
        List<Address> otherFollowers = new ArrayList<>(request.getFollowerAddresses());
        broker.otherFollowers.put(request.getQueueName(), otherFollowers); // store the other follower addresses
        response.setMessageType(MessageType.ACK);
        System.out.println("[INFO]: [Broker:" + broker.port + "] Replicated queue " + queueName);
        broker.electionHandler.createElectionTimeout(request.getQueueName(), true); //create scheduler in the pool, start timeout
    }

    private void handleAppendMessage(InterBrokerMessage request, InterBrokerMessage response) {
        String queueName = request.getQueueName();

        // Check if this is a replication queue
        if (this.broker.replications.get(queueName) == null) {
            System.out.println("[INFO]: [Broker:" + broker.port + "] Do not have this replication, cannot append message. Queue: " + queueName);
            return;
        }

        // Append the message to the replicated queue
        broker.replications.get(queueName).add(request.getData());
        response.setMessageType(MessageType.ACK);

        System.out.println("[INFO]: [Broker:" + broker.port + "] Replicated queue " + queueName +
                ": data " + request.getData() +
                " added to replication");
    }

    private void handleReadMessage(InterBrokerMessage request, InterBrokerMessage response) {
        String queueName = request.getQueueName();

        // Check if this is a replication queue
        if (this.broker.replications.get(queueName) == null) {
            System.out.println("Do not have this replication, cannot read message. Queue: " + queueName);
            return;
        }

        // Find the original client ID for this replication
        String originalClientId = request.getOriginalClientId();
        if (originalClientId == null) {
            System.out.println("No original client ID provided for replication read");
            return;
        }

        // Update replication client offsets
        Map<String, Integer> queueOffsets = broker.replicationClientOffsets
                .computeIfAbsent(queueName, k -> new HashMap<>());

        int currentOffset = queueOffsets.getOrDefault(originalClientId, 0);
        List<Integer> replicatedQueue = broker.replications.get(queueName);

        if (currentOffset < replicatedQueue.size()) {
            // Successfully read from replication queue
            response.setMessageType(MessageType.ACK);
            response.setData(replicatedQueue.get(currentOffset));

            // Increment offset for this client
            queueOffsets.put(originalClientId, currentOffset + 1);

            System.out.println("Replicated queue " + queueName +
                    ": read data " + replicatedQueue.get(currentOffset) +
                    " for client " + originalClientId);
        } else {
            // No more messages to read
            response.setMessageType(MessageType.NACK);
            System.out.println("No more messages in replicated queue " + queueName);
        }
    }

    private void handlePingMessage(InterBrokerMessage request, InterBrokerMessage response) {
        String queueName = request.getQueueName();
        broker.electionHandler.resetElectionTimeout(queueName);
        int currentTerm = broker.terms.get(queueName);
        int receivingTerm = request.getTerm();
        List<Address> newOtherFollowerAddresses = request.getFollowerAddresses();
        newOtherFollowerAddresses.remove(broker.brokerAddress);
        broker.otherFollowers.put(queueName, newOtherFollowerAddresses);

        // leader has changed
        if (receivingTerm > currentTerm) {
           broker.terms.put(queueName, receivingTerm);
           broker.electionHandler.createElectionTimeout(queueName, false);
           broker.queueAddressMap.put(queueName, request.getLeader());
           response.setMessageType(MessageType.ACK);
           return;
        }

        broker.terms.put(queueName, receivingTerm);
        response.setMessageType(MessageType.ACK);
    }
}
