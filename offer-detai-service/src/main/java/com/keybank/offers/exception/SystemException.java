package com.keybank.offers.exception;

import lombok.Data;

@Data
public class SystemException extends Exception {

    private String responseCode;
    private String responseMessage;

    public SystemException(String responseCode, String responseMessage) {
        this.responseCode = responseCode;
        this.responseMessage = responseMessage;
    }
}
