package client;
import common.Message;
import java.io.*;
import java.net.*;

public class Client {
    private String brokerAddress;
    private int brokerPort;

    public Client(String brokerAddress, int brokerPort) {
        this.brokerAddress = brokerAddress;
        this.brokerPort = brokerPort;
    }

    private Message sendRequest(Message request) {
        try {
            Socket socket = new Socket(brokerAddress, brokerPort);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(request);
            out.flush();
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            Message response = (Message) in.readObject();
            socket.close();
            return response;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void createQueue(String queueName) {
        Message request = new Message("create", queueName, null, null);
        Message response = sendRequest(request);
        if (response != null) {
            System.out.println("Create queue: " + response.getResponseType() + " - " + response.getResponseMessage());
        }
    }

    public void append(String queueName, Integer value) {
        Message request = new Message("append", queueName, null, value);
        Message response = sendRequest(request);
        if (response != null) {
            System.out.println("Append: " + response.getResponseType() + " - " + response.getResponseMessage());
        }
    }

    public Integer read(String queueName, String clientId) {
        Message request = new Message("read", queueName, clientId, null);
        Message response = sendRequest(request);
        if (response != null) {
            if ("success".equals(response.getResponseType())) {
                if (response.getResponseData() != null) {
                    System.out.println("Read value: " + response.getResponseData());
                    return response.getResponseData();
                } else {
                    System.out.println("No more messages");
                    return null;
                }
            } else {
                System.out.println("Read error: " + response.getResponseMessage());
                return null;
            }
        }
        return null;
    }

    public static void main(String[] args) {
        Client client = new Client("localhost", 5001);
        client.createQueue("testQueue");
//        client.append("testQueue", 42);
//        client.append("testQueue", 100);
        client.read("testQueue", "client1");
        client.read("testQueue", "client1");
        client.read("testQueue", "client1"); // Should indicate no more messages
    }
}