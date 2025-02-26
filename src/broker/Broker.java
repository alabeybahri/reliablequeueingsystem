package broker;
import common.Message;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Broker {
    private int port;
    Map<String, List<Integer>> queues = new ConcurrentHashMap<>();
    public Map<String, Map<String, Integer>> clientOffsets = new ConcurrentHashMap<>(); // clientId -> (queueName -> offset)
    private Set<String> currentClients = ConcurrentHashMap.newKeySet();
    private boolean isLeader;

    public Broker(int port, boolean isLeader) {
        this.port = port;
        this.isLeader = isLeader;
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Broker started on port " + port + (isLeader ? " (Leader)" : ""));
            while (true) {
                Socket socket = serverSocket.accept();
                String clientId = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
                currentClients.add(clientId);
                System.out.println("Client connected: " + clientId);
                new Thread(new ClientHandler(socket, clientId, this)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void removeClient(String clientId) {
        currentClients.remove(clientId);
        clientOffsets.remove(clientId);
        System.out.println("Client disconnected: " + clientId);
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        // Leader is the broker with the highest port (e.g., 5002)
        boolean isLeader = port == 5002;
        Broker broker = new Broker(port, isLeader);
        broker.start();
    }
}

class ClientHandler implements Runnable {
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
            case "create":
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
            case "write":
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
            case "read":
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