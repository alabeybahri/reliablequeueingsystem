package broker;

public class BrokerApplication {
    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        // Leader is the broker with the highest port (e.g., 5002)
        boolean isLeader = port == 5002;
        Broker broker = new Broker(port);
        broker.start();
    }
}
