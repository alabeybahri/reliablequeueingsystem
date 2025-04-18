package common;

import common.enums.MessageType;

import java.io.*;
import java.util.List;

public class InterBrokerMessage implements Serializable {
    private MessageType messageType;
    private Integer port;
    private String queueName;
    private Address leader;
    private int term;
    private boolean vote;
    private int data;
    private String originalClientId;
    private List<Address> followerAddresses;


    public Address getLeader() {
        return leader;
    }

    public void setLeader(Address leader) {
        this.leader = leader;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public byte[] serializeToBytes() throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(this);
            return bos.toByteArray();
        }
    }

    public static InterBrokerMessage deserializeFromBytes(byte[] bytes) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return (InterBrokerMessage) in.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public int getTerm() { return term; }

    public void setTerm(int term) { this.term = term; }

    public int getData() {
        return data;
    }

    public void setData(int data) {
        this.data = data;
    }

    public String getOriginalClientId() {
        return originalClientId;
    }

    public void setOriginalClientId(String originalClientId) {
        this.originalClientId = originalClientId;
    }

    public boolean isVote() {
        return vote;
    }

    public void setVote(boolean vote) {
        this.vote = vote;
    }

    public List<Address> getFollowerAddresses() {
        return followerAddresses;
    }

    public void setFollowerAddresses(List<Address> followerAddresses) {
        this.followerAddresses = followerAddresses;
    }
}





