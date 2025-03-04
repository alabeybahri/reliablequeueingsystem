package common.enums;

public enum ResponseType {
    SUCCESS("success"),
    ERROR("error"),
    FAIL("failure");

    private final String value;
    ResponseType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static ResponseType fromValue(String value) {
        for (ResponseType type : ResponseType.values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown response type: " + value);
    }

}

