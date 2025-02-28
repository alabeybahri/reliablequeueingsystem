package common;

import java.net.*;
import java.util.Enumeration;

public class LocalIP {
    public static InetAddress getLocalIP() throws SocketException {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface iface = interfaces.nextElement();
            if (iface.isLoopback() || !iface.isUp()) continue;
            Enumeration<InetAddress> addresses = iface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress addr = addresses.nextElement();
                if (!addr.isLoopbackAddress() && addr instanceof Inet4Address) {
                    return addr;
                }
            }
        }
        return InetAddress.getLoopbackAddress();
    }

    public static void main(String[] args) throws SocketException {
        System.out.println("Local IP: " + getLocalIP().getHostAddress());
    }
}