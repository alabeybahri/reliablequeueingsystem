package broker;

import java.io.IOException;

public class BrokerApplication {
    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(args[0]);
        Broker broker = new Broker(port);
        broker.start();
    }
}
