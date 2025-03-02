package broker;

import common.Message;
import common.Operation;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientHandler implements Runnable {
    private Socket socket;
    private String clientId;
    private Broker broker;
    private ObjectInputStream in;
    private ObjectOutputStream out;

    public ClientHandler(Socket socket, String clientId, Broker broker) {
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
                Message request = (Message) in.readObject();
                if (request == null) break;
                Message response = processRequest(request);
                out.writeObject(response);
                out.flush();
            }
        } catch (EOFException e) {
            // Client disconnected
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            broker.removeClient(clientId);
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleQueueCreate(Message request, Message response) throws IOException {
        if (this.broker.queueAddressMap.get(request.getQueueName()) != null) {
            response.setResponseType("failure");
            response.setResponseMessage("There is already a queue with this name");
        } else {
            broker.queues.put(request.getQueueName(), new ArrayList<>());
            this.broker.queueAddressMap.put(request.getQueueName(), this.broker.brokerAddress);
            response.setResponseType("success");
            response.setResponseMessage("Successfully added a queue with this name");
            broker.updateQueueAddressMap(request.getQueueName());
        }
    }

    private void handleQueueWrite(Message request, Message response) {
        if (this.broker.queues.get(request.getQueueName()) == null) {
            String leaderAddress = this.broker.queueAddressMap.get(request.getQueueName()).toString();
            if (leaderAddress == null) {
                response.setResponseType("failure");
                response.setResponseMessage("There is no queue");
            }
            else {
                response.setResponseType("success");
                response.setResponseMessage("This broker is not the leader of this queue, leader is: " + leaderAddress);
            }
        }
        else {
            broker.queues.get(request.getQueueName()).add(request.getValue());
            response.setResponseType("success");
            response.setResponseMessage("Written successfully");
        }
    }

    private void handleQueueRead(Message request, Message response) {
        if (this.broker.queues.get(request.getQueueName()) == null) {
            String leaderAddress = this.broker.queueAddressMap.get(request.getQueueName()).toString();
            if (leaderAddress == null) {
                response.setResponseType("failure");
                response.setResponseMessage("There is no queue");
            }
            else {
                response.setResponseType("success");
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
                response.setResponseType("success");
                response.setResponseData(value);
            } else {
                response.setResponseType("failure");
                response.setResponseMessage("No more messages");
            }
        }
    }



    private Message processRequest(Message request) throws IOException {
        String type = request.getType();
        String queueName = request.getQueueName();
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
                response.setResponseType("error");
                response.setResponseMessage("Invalid request type");
        }
        return response;
    }
}