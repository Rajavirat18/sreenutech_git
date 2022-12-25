package com.keybank.offers.exception;

import lombok.Data;
import org.springframework.stereotype.Component;
@Data
@Component
public class OffersRequestInvalidException extends Exception{

    private String responseCode;
    private String responseMessage;

    public OffersRequestInvalidException(){}

    public OffersRequestInvalidException(String responseCode,String responseMessage){
        this.responseCode = responseCode;
        this.responseMessage = responseMessage;
    }

}
