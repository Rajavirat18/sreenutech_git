package com.keybank.offers.exception;

import lombok.Data;

@Data
public class BussinessException extends Exception {

    private String responseCode;
    private String responseMessage;

    public BussinessException(String responseCode, String responseMessage) {
        this.responseCode = responseCode;
        this.responseMessage = responseMessage;

    }
}
