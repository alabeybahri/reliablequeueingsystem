package common;

import java.io.Serializable;
import java.util.Objects;

public class Address implements Serializable {
    private String host;
    private int port;

    public Address(String host, int port) {
        this.host = host;
        this.port = port;
    }


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String toString() {
        return host + ":" + port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Address other = (Address) o;

        String thisHost = normalizeHost(this.host);
        String otherHost = normalizeHost(other.host);

        return this.port == other.port && Objects.equals(thisHost, otherHost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }


    private String normalizeHost(String host) {
        if (host == null) return null;
        return host.startsWith("/") ? host.substring(1) : host;
    }

}
