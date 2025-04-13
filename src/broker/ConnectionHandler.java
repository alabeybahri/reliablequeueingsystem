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
import java.net.SocketTimeoutException;
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
            System.err.println("[ERROR]: Connection error");
        }
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    socket.setSoTimeout(30000);

                    Object request = in.readObject();
                    if (request == null) {
                        System.out.println("[INFO]: [Broker:" + broker.port + "] Received null request, closing connection");
                        break;
                    }

                    if (request instanceof InterBrokerMessage) {
                        InterBrokerMessage brokerRequest = (InterBrokerMessage) request;

                        InterBrokerMessage responseToBroker = processBrokerRequest(brokerRequest);
                        out.reset();
                        out.writeObject(responseToBroker);
                        out.flush();

                    } else if (request instanceof Message) {
                        Message clientRequest = (Message) request;
                        Message responseToClient = processClientRequest(clientRequest);
                        out.reset();
                        out.writeObject(responseToClient);
                        out.flush();

                    } else {
                        System.err.println("[ERROR]: Received unknown request type.");
                    }
                } catch (SocketTimeoutException e) {
                    continue;
                } catch (EOFException e) {
                    break;
                } catch (ClassNotFoundException e) {
                    System.err.println("[ERROR]: Unknown object type received: " + e.getMessage());
                } catch (IOException e) {
                    System.err.println("[ERROR]: IO error while processing request: " + e.getMessage());
                    break;
                } catch (Exception e) {
                    System.err.println("[ERROR]: Unexpected error processing request: " + e.getMessage());
                    break;
                }
            }
        } finally {
            System.out.println("[INFO]: [Broker:" + broker.port + "] Connection handler exiting for: " + connectionAddress);
            broker.removeClient(connectionAddress);
            closeResources();
        }
    }

    private void closeResources() {
        try {
            if (in != null) in.close();
            if (out != null) out.close();
            if (socket != null && !socket.isClosed()) socket.close();
        } catch (IOException e) {
            System.err.println("[ERROR]: Error closing resources: " + e.getMessage());
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
            response.setResponseMessage("Successfully added a queue with name " + request.getQueueName());
            broker.terms.put(request.getQueueName(), 1);
            broker.updateQueueAddressMap(request.getQueueName(), MessageType.NEW_QUEUE, 1);
            broker.startPeriodicPingFollowers(request.getQueueName());
        }
    }

    private void handleQueueWrite(Message request, Message response) {
        String queueName = request.getQueueName();
        if (this.broker.queues.get(request.getQueueName()) == null) {
            Address leaderAddress = this.broker.queueAddressMap.get(queueName);
            if (leaderAddress == null) {
                response.setResponseType(ResponseType.ERROR);
                response.setResponseMessage("There is no queue with name :" + queueName);
            }
            else {
                InterBrokerMessage leaderResponse =  broker.forwardClientWriteMessage(Address.normalizeAddress(leaderAddress), queueName, request);

                if (leaderResponse.getMessageType() == MessageType.ACK) {
                    response.setResponseType(ResponseType.SUCCESS);
                    response.setResponseMessage("Written successfully");
                } else {
                    response.setResponseType(ResponseType.ERROR);
                    response.setResponseMessage("Error while processing queue write");
                }
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
        String queueName = request.getQueueName();

        if (this.broker.queues.get(queueName) == null) {
            Address leaderAddress = this.broker.queueAddressMap.get(queueName);

            if (leaderAddress == null) {
                response.setResponseType(ResponseType.ERROR);
                response.setResponseMessage("There is no queue with name: " + queueName);
                return;
            }

            InterBrokerMessage leaderResponse = broker.forwardClientReadMessage(Address.normalizeAddress(leaderAddress), queueName, request.getClientId());

            if (leaderResponse.getMessageType() == MessageType.ACK) {
                response.setResponseType(ResponseType.SUCCESS);
                response.setResponseData(leaderResponse.getData());
            } else {
                response.setResponseType(ResponseType.ERROR);
                response.setResponseMessage("No more messages");
            }
        }
        else {
            List<Integer> queue = broker.queues.get(queueName);
            Map<String, Integer> offsets = broker.clientOffsets.computeIfAbsent(queueName, k -> new HashMap<>());
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
            case BROKER_READ:
                handleBrokerReadMessage(request, response);
                break;
            case BROKER_WRITE:
                handleBrokerWriteMessage(request, response);
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

    private void handleBrokerWriteMessage(InterBrokerMessage request, InterBrokerMessage response) {
        String queueName = request.getQueueName();

        if (this.broker.queues.get(queueName) == null) {
            response.setMessageType(MessageType.NACK);
            response.setData(-1);
            return;
        }

        broker.queues.get(request.getQueueName()).add(request.getData());

        Message dummyMessage = new Message();
        dummyMessage.setQueueName(queueName);
        dummyMessage.setValue(request.getData());
        dummyMessage.setType(Operation.WRITE);

        broker.updateReplications(dummyMessage, request.getOriginalClientId());

        response.setMessageType(MessageType.ACK);

    }

    private void handleBrokerReadMessage(InterBrokerMessage request, InterBrokerMessage response) {
        String queueName = request.getQueueName();
        String originalClientId = request.getOriginalClientId();

        if (this.broker.queues.get(queueName) == null) {
            response.setMessageType(MessageType.NACK);
            response.setData(-1);
            return;
        }

        List<Integer> queue = broker.queues.get(queueName);
        Map<String, Integer> offsets = broker.clientOffsets.computeIfAbsent(queueName, k -> new HashMap<>());
        int offset = offsets.getOrDefault(originalClientId, 0);

        if (offset < queue.size()) {
            Integer value = queue.get(offset);
            offsets.put(originalClientId, offset + 1);

            Message dummyMessage = new Message();
            dummyMessage.setQueueName(queueName);
            dummyMessage.setType(Operation.READ);


            broker.updateReplications(dummyMessage, originalClientId);

            response.setMessageType(MessageType.ACK);
            response.setData(value);
        } else {
            response.setMessageType(MessageType.NACK);
            response.setData(-1);
        }
    }

    private void handleElectionMessage(InterBrokerMessage request, InterBrokerMessage response) {
        String queueName = request.getQueueName();
        int requestTerm = request.getTerm();

        if (!broker.terms.containsKey(queueName)) {
            broker.terms.put(queueName, 0);
        }
        int currentTerm = broker.terms.get(queueName);

        if (requestTerm > currentTerm) {
            broker.terms.put(queueName, requestTerm);
            response.setMessageType(MessageType.VOTE);
            response.setVote(true);

            broker.electionHandler.cancelElectionTimeout(queueName);
            broker.electionHandler.resetElectionTimeout(queueName);

            broker.votesGranted.computeIfAbsent(queueName, k -> new HashSet<>()).add(requestTerm);

            System.out.println("[INFO]: [Broker: " + broker.port + "] Vote granted for " + queueName + " with term " + requestTerm);
            return;
        }

        response.setMessageType(MessageType.VOTE);
        response.setVote(false);
        System.out.println("[INFO]: [Broker: " + broker.port + "] Vote denied for " + queueName + " with term " + requestTerm + ", our term: " + currentTerm);
    }

    private void handleReplicationRequest(InterBrokerMessage request, InterBrokerMessage response) {
        String queueName = request.getQueueName();

        if (this.broker.queues.get(queueName) != null) {
            System.err.println("[ERROR]: [Broker:" + broker.port + "] Leader of this queue, should not take this replication request: " + queueName);
            response.setMessageType(MessageType.NACK);
            return;
        }

        broker.replications.put(queueName, new ArrayList<>());
        broker.terms.put(queueName, 1);
        Map<String, Integer> replicationOffset = new HashMap<>();
        broker.replicationClientOffsets.put(queueName, replicationOffset);
        List<Address> otherFollowers = new ArrayList<>(request.getFollowerAddresses());
        otherFollowers.remove(broker.brokerAddress);
        broker.otherFollowers.put(request.getQueueName(), otherFollowers);
        response.setMessageType(MessageType.ACK);
        System.out.println("[INFO]: [Broker:" + broker.port + "] Replicated queue " + queueName);
        broker.electionHandler.createElectionTimeout(request.getQueueName());
    }

    private void handleAppendMessage(InterBrokerMessage request, InterBrokerMessage response) {
        String queueName = request.getQueueName();

        if (this.broker.replications.get(queueName) == null) {
            System.out.println("[INFO]: [Broker:" + broker.port + "] Do not have this replication, cannot append message. Queue: " + queueName);
            return;
        }

        broker.replications.get(queueName).add(request.getData());
        response.setMessageType(MessageType.ACK);

        System.out.println("[INFO]: [Broker:" + broker.port + "] Replicated queue " + queueName + ", " + request.getData() + " added to replication");
    }

    private void handleReadMessage(InterBrokerMessage request, InterBrokerMessage response) {
        String queueName = request.getQueueName();

        if (this.broker.replications.get(queueName) == null) {
            System.err.println("[ERROR]: [Broker:" + broker.port + "]Do not have this replication, cannot read message. Queue: " + queueName);
            return;
        }

        String originalClientId = request.getOriginalClientId();
        if (originalClientId == null) {
            System.err.println("[ERROR]: [Broker:" + broker.port + "] No original client ID provided for replication read");
            return;
        }

        Map<String, Integer> queueOffsets = broker.replicationClientOffsets
                .computeIfAbsent(queueName, k -> new HashMap<>());

        int currentOffset = queueOffsets.getOrDefault(originalClientId, 0);
        List<Integer> replicatedQueue = broker.replications.get(queueName);

        if (currentOffset < replicatedQueue.size()) {
            response.setMessageType(MessageType.ACK);
            response.setData(replicatedQueue.get(currentOffset));

            queueOffsets.put(originalClientId, currentOffset + 1);

            System.out.println("[INFO]: [Broker:" + broker.port + "] Replicated queue " + queueName + ": read data " + replicatedQueue.get(currentOffset) + " for client " + originalClientId);
        } else {
            response.setMessageType(MessageType.NACK);
            System.out.println("[INFO]: [Broker:" + broker.port + "] No more messages in replicated queue " + queueName);
        }
    }

    private void handlePingMessage(InterBrokerMessage request, InterBrokerMessage response) {
        String queueName = request.getQueueName();
        int currentTerm = 0;
        if (broker.terms.containsKey(queueName)) {
            currentTerm = broker.terms.get(queueName);
        }
        int receivingTerm = request.getTerm();
        List<Address> newOtherFollowerAddresses = request.getFollowerAddresses();
        newOtherFollowerAddresses.remove(broker.brokerAddress);
        broker.otherFollowers.put(queueName, newOtherFollowerAddresses);

        if (receivingTerm >= currentTerm) {
            broker.terms.put(queueName, receivingTerm);
            broker.queueAddressMap.put(queueName, request.getLeader());
            broker.electionHandler.cancelElectionTimeout(queueName);
            broker.electionHandler.resetElectionTimeout(queueName);
            response.setMessageType(MessageType.ACK);
            return;
        }

        response.setMessageType(MessageType.ACK);
    }

}
