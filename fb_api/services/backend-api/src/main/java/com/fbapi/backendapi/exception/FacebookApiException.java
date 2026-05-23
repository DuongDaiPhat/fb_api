package com.fbapi.backendapi.exception;

public class FacebookApiException extends RuntimeException {
    private final String errorCode;

    public FacebookApiException(String message, String errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }
}
