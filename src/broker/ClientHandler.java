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

    private Message processRequest(Message request) {
        String type = request.getType();
        String queueName = request.getQueueName();
        Message response = new Message();
        response.setResponseType("success");

        switch (type) {
            case Operation.CREATE:
                synchronized (broker.queues) {
                    if (!broker.queues.containsKey(queueName)) {
                        broker.queues.put(queueName, new ArrayList<>());
                        response.setResponseMessage("Queue created");
                    } else {
                        response.setResponseType("error");
                        response.setResponseMessage("Queue already exists");
                    }
                }
                break;
            case Operation.WRITE:
                synchronized (broker.queues) {
                    if (broker.queues.containsKey(queueName)) {
                        broker.queues.get(queueName).add(request.getValue());
                        response.setResponseMessage("Written successfully");
                    } else {
                        response.setResponseType("error");
                        response.setResponseMessage("Queue does not exist");
                    }
                }
                break;
            case Operation.READ:
                synchronized (broker.queues) {
                    if (broker.queues.containsKey(queueName)) {
                        List<Integer> queue = broker.queues.get(queueName);
                        Map<String, Integer> offsets = broker.clientOffsets.computeIfAbsent(clientId, k -> new HashMap<>());
                        int offset = offsets.getOrDefault(queueName, 0);
                        if (offset < queue.size()) {
                            Integer value = queue.get(offset);
                            offsets.put(queueName, offset + 1);
                            response.setResponseData(value);
                        } else {
                            response.setResponseMessage("No more messages");
                        }
                    } else {
                        response.setResponseType("error");
                        response.setResponseMessage("Queue does not exist");
                    }
                }
                break;
            default:
                response.setResponseType("error");
                response.setResponseMessage("Invalid request type");
        }
        return response;
    }
}