package common;

import java.net.*;
import java.util.Enumeration;

public class LocalIP {
    public static InetAddress getLocalIP() throws SocketException {
        // Try to get a non-loopback, non-link-local address first
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface iface = interfaces.nextElement();
            // Skip inactive interfaces and loopback
            if (iface.isLoopback() || !iface.isUp()) continue;

            Enumeration<InetAddress> addresses = iface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress addr = addresses.nextElement();
                // Look for IPv4 addresses that are not link-local (169.254.x.x)
                if (addr instanceof Inet4Address && !addr.isLoopbackAddress()
                        && !addr.getHostAddress().startsWith("169.254")) {
                    return addr;
                }
            }
        }

        // If no suitable address found, fall back to any IPv4 address
        interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface iface = interfaces.nextElement();
            if (iface.isLoopback() || !iface.isUp()) continue;

            Enumeration<InetAddress> addresses = iface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress addr = addresses.nextElement();
                if (addr instanceof Inet4Address && !addr.isLoopbackAddress()) {
                    return addr;
                }
            }
        }

        // If still nothing, return loopback address
        return InetAddress.getLoopbackAddress();
    }

    public static void main(String[] args) throws SocketException {
        System.out.println("Local IP: " + getLocalIP().getHostAddress());
    }
}