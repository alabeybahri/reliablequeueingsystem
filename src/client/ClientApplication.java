package client;

import java.net.SocketException;

public class ClientApplication {
    public static void main(String[] args) throws SocketException {
        Client client = new Client();
        client.start();
    }
}
