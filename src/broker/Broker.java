package broker;
import common.Message;
import java.io.*;
import java.net.*;
import java.util.*;

public class Broker {
    private int port;
    private Map<String, List<Integer>> queues;           // Queue name -> list of messages
    private Map<String, Map<String, Integer>> offsets;   // Queue name -> (clientId -> offset)

    public Broker(int port) {
        this.port = port;
        this.queues = new HashMap<>();
        this.offsets = new HashMap<>();
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Broker started on port " + port);
            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(() -> handleClient(socket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleClient(Socket socket) {
        try {
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            Message request = (Message) in.readObject();
            Message response = processRequest(request);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(response);
            out.flush();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
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
        Message response;

        // Synchronize to ensure thread safety
        synchronized (this) {
            if ("create".equals(type)) {
                if (!queues.containsKey(queueName)) {
                    queues.put(queueName, new ArrayList<>());
                    offsets.put(queueName, new HashMap<>());
                    response = new Message("success", "Queue created", null);
                } else {
                    response = new Message("error", "Queue already exists", null);
                }
            } else if ("append".equals(type)) {
                if (queues.containsKey(queueName)) {
                    queues.get(queueName).add(request.getData());
                    response = new Message("success", "Appended successfully", null);
                } else {
                    response = new Message("error", "Queue does not exist", null);
                }
            } else if ("read".equals(type)) {
                if (queues.containsKey(queueName)) {
                    Map<String, Integer> clientOffsets = offsets.get(queueName);
                    String clientId = request.getClientId();
                    int offset = clientOffsets.getOrDefault(clientId, 0);
                    List<Integer> queue = queues.get(queueName);
                    if (offset < queue.size()) {
                        Integer value = queue.get(offset);
                        clientOffsets.put(clientId, offset + 1);
                        response = new Message("success", null, value);
                    } else {
                        response = new Message("success", "No more messages", null);
                    }
                } else {
                    response = new Message("error", "Queue does not exist", null);
                }
            } else {
                response = new Message("error", "Invalid request type", null);
            }
        }
        return response;
    }

    public static void main(String[] args) {
        Broker broker = new Broker(5001);
        broker.start();
    }
}