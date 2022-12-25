package com.keybank.offers.controller;

import com.keybank.offers.exception.BussinessException;
import com.keybank.offers.exception.OffersRequestInvalidException;
import com.keybank.offers.exception.SystemException;
import com.keybank.offers.model.OffersResponse;
import com.keybank.offers.model.StatusBlock;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class OffersDetailsControllerAdvice {

    @ExceptionHandler(value = OffersRequestInvalidException.class)
    public OffersResponse handleRequestInavlidException(OffersRequestInvalidException offersRequestInvalidException) {

        OffersResponse offersResponse = buildOffersResponse(offersRequestInvalidException.getResponseCode(), offersRequestInvalidException.getResponseMessage());
        return offersResponse;
    }


    @ExceptionHandler(value = BussinessException.class)
    public OffersResponse handleBussingException(BussinessException bussinessException) {

        OffersResponse offersResponse = buildOffersResponse(bussinessException.getResponseCode(), bussinessException.getResponseMessage());
        return offersResponse;
    }

    @ExceptionHandler(value = SystemException.class)
    public OffersResponse hanmdleSystemException(SystemException systemException) {
        OffersResponse offersResponse = buildOffersResponse(systemException.getResponseCode(), systemException.getResponseMessage());
        return offersResponse;
    }

    @ExceptionHandler(value = Exception.class)
    public OffersResponse hanmdleGenericException(Exception exception) {
        OffersResponse offersResponse = buildOffersResponse("777", "Generic error");
        return offersResponse;
    }

    private OffersResponse buildOffersResponse(String responseCode, String responseMessage) {
        OffersResponse offersResponse = new OffersResponse();

        StatusBlock statusBlock = new StatusBlock();
        statusBlock.setResponseCode(responseCode);
        statusBlock.setResponseMessage(responseMessage);

        offersResponse.setStatusBlock(statusBlock);
        return offersResponse;
    }
}
