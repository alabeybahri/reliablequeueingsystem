package common.enums;

public enum MessageType {
    ELECTION("ELECTION"),
    PING("PING"),
    VOTE("VOTE"),
    DISCOVER("DISCOVER"),
    LEADER_ANNOUNCEMENT("LEADER_ANNOUNCEMENT"),
    APPEND_MESSAGE("APPEND_MESSAGE"),
    READ_MESSAGE("READ_MESSAGE"),
    NEW_QUEUE("NEW_QUEUE"),
    BROKER_READ("BROKER_READ"),
    BROKER_WRITE("BROKER_WRITE"),
    ACK("ACK"),
    REPLICATION("REPLICATION"),
    NACK("NACK");


    private final String value;

    MessageType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static MessageType fromValue(String value) {
        for (MessageType type : MessageType.values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown message type: " + value);
    }
}
