package client;
import common.Address;
import common.Message;
import common.Operation;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
            return;
        }
        //randomly connect one.
        Address brokerAddress = brokerCache.get(new Random().nextInt(brokerCache.size()));
        connectToBroker(brokerAddress.getIp(), brokerAddress.getPort());
    }

    public void start() {
        if (socket == null || !socket.isConnected()) {
            System.out.println("Not connected to any broker. Exiting.");
            return;
        }
        while (true) {
            String[] parts = getCommand();
            if (parts.length == 0) {continue;}
            String command = parts[0];
            Message request = new Message();

            try {
                switch (command) {
                    case Operation.CREATE:
                        if (parts.length != 2) {
                            System.out.println("Usage: create <queue_name>");
                            continue;
                        }
                        request.setType(Operation.CREATE);
                        request.setQueueName(parts[1]);
                        break;
                    case Operation.READ:
                        if (parts.length != 2) {
                            System.out.println("Usage: read <queue_name>");
                            continue;
                        }
                        request.setType(Operation.READ);
                        request.setQueueName(parts[1]);
                        break;
                    case Operation.WRITE:
                        if (parts.length != 3) {
                            System.out.println("Usage: write <queue_name> <value>");
                            continue;
                        }
                        request.setType(Operation.WRITE);
                        request.setQueueName(parts[1]);
                        request.setValue(Integer.parseInt(parts[2]));
                        break;
                    default:
                        System.out.println("Invalid command. Use: create, read, write");
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
            socket = new Socket(host, port);
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            System.out.println("Connected to broker at " + host + ":" + port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void discoverBrokers() {
        try {
            MulticastSocket multicastSocket = new MulticastSocket();
            multicastSocket.setSoTimeout(DISCOVERY_TIMEOUT);
            InetAddress group = InetAddress.getByName(MULTICAST_ADDRESS);


            // Send discovery message
            String message = "DISCOVER_BROKERS";
            byte[] messageBytes = message.getBytes();
            DatagramPacket packet = new DatagramPacket(messageBytes, messageBytes.length, group, MULTICAST_PORT);
            multicastSocket.send(packet);
            System.out.println("Sent discovery request to " + MULTICAST_ADDRESS + ":" + MULTICAST_PORT);

            // Receive responses
            while (true) {
                try {
                    byte[] buffer = new byte[256];
                    DatagramPacket responsePacket = new DatagramPacket(buffer, buffer.length);
                    multicastSocket.receive(responsePacket);
                    String response = new String(responsePacket.getData(), 0, responsePacket.getLength());
                    String[] parts = response.split(":");
                    if (parts.length == 2) {
                        String host = parts[0];
                        int port = Integer.parseInt(parts[1]);
                        brokerCache.add(new Address(host, port));
                        System.out.println("Discovered broker at " + host + ":" + port);
                    }
                } catch (SocketTimeoutException e) {
                    System.out.println("Discovery complete. Found " + brokerCache.size() + " brokers.");
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