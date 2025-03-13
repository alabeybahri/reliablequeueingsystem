package common;
import common.enums.ResponseType;

import java.io.Serializable;

public class Message implements Serializable {
    private String type; // "create", "read", "write"
    private String queueName;
    private Integer value; // for write operations
    private ResponseType responseType; // "success", "error"
    private String responseMessage;
    private Integer responseData; // for read operations
    private String clientId;

    public Message() {}

    public Message(String type, String queueName, Integer value) {
        this.type = type;
        this.queueName = queueName;
        this.value = value;
    }

    // Getters and setters
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getQueueName() { return queueName; }
    public void setQueueName(String queueName) { this.queueName = queueName; }
    public Integer getValue() { return value; }
    public void setValue(Integer value) { this.value = value; }
    public ResponseType getResponseType() { return responseType; }
    public void setResponseType(ResponseType responseType) { this.responseType = responseType; }
    public String getResponseMessage() { return responseMessage; }
    public void setResponseMessage(String responseMessage) { this.responseMessage = responseMessage; }
    public Integer getResponseData() { return responseData; }
    public void setResponseData(Integer responseData) { this.responseData = responseData; }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
}