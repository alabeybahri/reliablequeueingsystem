package client;
import common.Message;
import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Client {
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    public Client(String brokerAddress, int brokerPort) {
        try {
            socket = new Socket(brokerAddress, brokerPort);
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            System.out.println("Connected to broker at " + brokerAddress + ":" + brokerPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("> ");
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) continue;
            String[] parts = line.split(" ");
            String command = parts[0];
            Message request = new Message();

            try {
                switch (command) {
                    case "create":
                        if (parts.length != 2) {
                            System.out.println("Usage: create <queue_name>");
                            continue;
                        }
                        request.setType("create");
                        request.setQueueName(parts[1]);
                        break;
                    case "read":
                        if (parts.length != 2) {
                            System.out.println("Usage: read <queue_name>");
                            continue;
                        }
                        request.setType("read");
                        request.setQueueName(parts[1]);
                        break;
                    case "write":
                        if (parts.length != 3) {
                            System.out.println("Usage: write <queue_name> <value>");
                            continue;
                        }
                        request.setType("write");
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

    public static void main(String[] args) {
        Client client = new Client("localhost", 5001); // Connect to broker on port 5000
        client.start();
    }
}