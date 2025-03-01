package client;
import common.Address;
import common.Message;
import common.Operation;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Client {
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private Scanner scanner;

    // Multicast configuration
    private static final String MULTICAST_ADDRESS = "224.0.0.1";
    private static final int MULTICAST_PORT = 5010;
    private static final int DISCOVERY_TIMEOUT = 1000; // 1 second timeout
    private List<Address> brokerCache = new ArrayList<>();

    public Client() {
        scanner = new Scanner(System.in);
        discoverBrokers();
        if (brokerCache.isEmpty()) {
            System.out.println("No brokers found.");
        }
    }

    public void start() {
        while (true) {
            String[] parts = getCommand();
            if (parts.length == 0) {continue;}
            String command = parts[0];
            Message request = new Message();

            try {
                switch (command) {
                    case Operation.CREATE:
                        if (socket == null || !socket.isConnected()) {
                            System.out.println("Not connected to any broker. ");
                            continue;
                        }
                        if (parts.length != 2) {
                            System.out.println("Usage: create <queue_name>");
                            continue;
                        }
                        request.setType(Operation.CREATE);
                        request.setQueueName(parts[1]);
                        break;
                    case Operation.READ:
                        if (socket == null || !socket.isConnected()) {
                            System.out.println("Not connected to any broker. ");
                            continue;
                        }
                        if (parts.length != 2) {
                            System.out.println("Usage: read <queue_name>");
                            continue;
                        }
                        request.setType(Operation.READ);
                        request.setQueueName(parts[1]);
                        break;
                    case Operation.WRITE:
                        if (socket == null || !socket.isConnected()) {
                            System.out.println("Not connected to any broker. ");
                            continue;
                        }
                        if (parts.length != 3) {
                            System.out.println("Usage: write <queue_name> <value>");
                            continue;
                        }
                        request.setType(Operation.WRITE);
                        request.setQueueName(parts[1]);
                        request.setValue(Integer.parseInt(parts[2]));
                        break;
                    case Operation.CONNECT:
                        if (parts.length != 3 && parts.length != 2) {
                            System.out.println("Usage: connect <host> <port>\nconnect <index>");
                            continue;
                        }
                        if (parts.length == 3) {
                            connectToBroker(parts[1], Integer.parseInt(parts[2]));
                        }
                        else {
                            connectToBroker(Integer.parseInt(parts[1]));
                        }
                        continue;
                    case Operation.DISCONNECT:
                        if (socket == null || !socket.isConnected()) {
                            System.out.println("Not connected to any broker.");
                            continue;
                        }
                        if (parts.length != 1){
                            System.out.println("Usage: disconnect");
                            continue;
                        }
                        disconnectFromBroker();
                        discoverBrokers();
                        continue;
                    default:
                        System.out.println("Invalid command. Use: create, read, write, connect!");
                        continue;
                }
                out.writeObject(request);
                out.flush();
                Message response = (Message) in.readObject();
                if ("success".equals(response.getResponseType())) {
                    if (response.getResponseData() != null) {
                        System.out.println("Received: " + response.getResponseData());
                    } else {
                        System.out.println(response.getResponseMessage());
                    }
                } else {
                    System.out.println("Error: " + response.getResponseMessage());
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }


    private void connectToBroker(String host, int port) {
        try {
            if (socket != null && !socket.isClosed()) {socket.close();}
            socket = new Socket(host, port);
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            System.out.println("Connected to broker at " + host + ":" + port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void connectToBroker(int index){
        try {
            if (socket != null && !socket.isClosed()) {socket.close();}
            String host = brokerCache.get(index - 1).getIp();
            int port = brokerCache.get(index - 1).getPort();
            socket = new Socket(host, port);
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            System.out.println("Connected to broker at " + host + ":" + port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void disconnectFromBroker() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void discoverBrokers() {
        try {
            brokerCache.clear();
            MulticastSocket multicastSocket = new MulticastSocket();
            multicastSocket.setSoTimeout(DISCOVERY_TIMEOUT);
            InetAddress group = InetAddress.getByName(MULTICAST_ADDRESS);


            // Send discovery message
            String message = "DISCOVER_BROKERS";
            byte[] messageBytes = message.getBytes();
            DatagramPacket packet = new DatagramPacket(messageBytes, messageBytes.length, group, MULTICAST_PORT);
            multicastSocket.send(packet);

            // Receive responses
            while (true) {
                try {
                    byte[] buffer = new byte[256];
                    DatagramPacket responsePacket = new DatagramPacket(buffer, buffer.length);
                    multicastSocket.receive(responsePacket);
                    Integer brokerHost = Integer.parseInt(new String(responsePacket.getData(), 0, responsePacket.getLength()));
                    String brokerIP = responsePacket.getAddress().getHostAddress();
                    brokerCache.add(new Address(brokerIP, brokerHost));
                    System.out.println("Discovered broker at " + brokerIP + ":" + brokerHost);

                } catch (SocketTimeoutException e) {
                    System.out.println("Discovery complete. Found " + brokerCache.size() + " brokers.\nYou can connect any of them by specifying the ip and the port or the broker index.");
                    break; // Stop after timeout
                }
            }
            multicastSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private String[] getCommand() {
        System.out.print("> ");
        String line = scanner.nextLine().trim();
        String[] parts = line.split(" ");
        return parts;

    }

}