package broker;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionHandler implements Runnable {
    private Socket socket;
    private String clientId;
    private Broker broker;
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private Map<String, String> replicationClientMap = new ConcurrentHashMap<>();

    public ConnectionHandler(Socket socket, String clientId, Broker broker) {
        this.socket = socket;
        this.clientId = clientId;
        this.broker = broker;
        try {
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
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
            broker.removeClient(clientId);
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
            broker.updateQueueAddressMap(request.getQueueName());
        }
    }

    private void handleQueueWrite(Message request, Message response) {
        if (this.broker.queues.get(request.getQueueName()) == null) {
            String leaderAddress = this.broker.queueAddressMap.get(request.getQueueName()).toString();
            if (leaderAddress == null) {
                response.setResponseType(ResponseType.ERROR);
                response.setResponseMessage("There is no queue");
            }
            else {
                // if there is replication, send message to replications
                broker.updateReplications(request, this.clientId);
                response.setResponseMessage("This broker is not the leader of this queue, leader is: " + leaderAddress);
            }
        }
        else {
            broker.queues.get(request.getQueueName()).add(request.getValue());
            broker.updateReplications(request, this.clientId);
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
            Map<String, Integer> offsets = broker.clientOffsets.computeIfAbsent(clientId, k -> new HashMap<>());
            int offset = offsets.getOrDefault(request.getQueueName(), 0);
            if (offset < queue.size()) {
                Integer value = queue.get(offset);
                offsets.put(request.getQueueName(), offset + 1);
                broker.updateReplications(request, this.clientId);
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
        return response;
    }


    //TODO: burda senkronizasyon halledilmeli
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
            default:
                response.setMessageType(MessageType.NACK);
                break;
        }
        return response;
    }

    private void handleReplicationRequest(InterBrokerMessage request, InterBrokerMessage response) {
        String queueName = request.getQueueName();

        // Check if queue already exists
        if (this.broker.queues.get(queueName) != null) {
            System.out.println("Leader of this queue, should not take this replication request: " + queueName);
            response.setMessageType(MessageType.NACK);
            return;
        }

        // Initialize replication for the queue
        broker.replications.put(queueName, new ArrayList<>());

        // Initialize client offsets for this replication
        Map<String, Integer> replicationOffset = new HashMap<>();
        broker.replicationClientOffsets.put(queueName, replicationOffset);

        response.setMessageType(MessageType.ACK);
        System.out.println("Replicated queue " + queueName);
    }

    private void handleAppendMessage(InterBrokerMessage request, InterBrokerMessage response) {
        String queueName = request.getQueueName();

        // Check if this is a replication queue
        if (this.broker.replications.get(queueName) == null) {
            System.out.println("Do not have this replication, cannot append message. Queue: " + queueName);
            return;
        }

        // Append the message to the replicated queue
        broker.replications.get(queueName).add(request.getData());
        response.setMessageType(MessageType.ACK);

        System.out.println("Replicated queue " + queueName +
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
}
