package common;

import java.io.Serializable;

public class Message implements Serializable {
    private String type;         // Request type: "create", "append", "read"
    private String queueName;    // Name of the queue
    private String clientId;     // Client identifier (for offset tracking in read)
    private Integer data;        // Data to append (null for create/read)
    private String responseType; // Response status: "success", "error"
    private String responseMessage; // Response message
    private Integer responseData;   // Data returned from read (null if none)

    // Constructor for requests
    public Message(String type, String queueName, String clientId, Integer data) {
        this.type = type;
        this.queueName = queueName;
        this.clientId = clientId;
        this.data = data;
    }

    // Constructor for responses
    public Message(String responseType, String responseMessage, Integer responseData) {
        this.responseType = responseType;
        this.responseMessage = responseMessage;
        this.responseData = responseData;
    }

    // Getters and setters
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getQueueName() { return queueName; }
    public void setQueueName(String queueName) { this.queueName = queueName; }
    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }
    public Integer getData() { return data; }
    public void setData(Integer data) { this.data = data; }
    public String getResponseType() { return responseType; }
    public void setResponseType(String responseType) { this.responseType = responseType; }
    public String getResponseMessage() { return responseMessage; }
    public void setResponseMessage(String responseMessage) { this.responseMessage = responseMessage; }
    public Integer getResponseData() { return responseData; }
    public void setResponseData(Integer responseData) { this.responseData = responseData; }
}