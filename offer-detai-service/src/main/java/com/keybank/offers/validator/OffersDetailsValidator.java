package com.keybank.offers.validator;

import com.keybank.offers.exception.OffersRequestInvalidException;
import com.keybank.offers.model.OffersRequest;
import org.springframework.stereotype.Component;

@Component
public class OffersDetailsValidator {

    public void validateRequest(OffersRequest offersRequest) throws OffersRequestInvalidException {

        if (offersRequest == null) {
            throw new OffersRequestInvalidException("ofs001", "Request object is invalid");
        }

        if (offersRequest.getRequestId().equals(null) || " ".equals(offersRequest.getRequestId()) ||
                offersRequest.getRequestId().trim().length() == 0 || "".equals(offersRequest.getRequestId())) {
            throw new OffersRequestInvalidException("ofs002", "requestId is not valid");
        }

        if (offersRequest.getCardNum().equals(null) || offersRequest.getCardNum().length() < 16
                || " ".equals(offersRequest.getCardNum()) || offersRequest.getCardNum().length() == 0) {
            throw new OffersRequestInvalidException("ofs003", "cardNumber is invalid");
        }

        if (offersRequest.getExpDate().equals(null) || " ".equals(offersRequest.getExpDate()) ||
                offersRequest.getExpDate().trim().length() == 0 || "".equals(offersRequest.getExpDate())) {
            throw new OffersRequestInvalidException("ofs003", "card expiure date is invalid");
        }

        if (offersRequest.getChannelId().equals(null) || " ".equals(offersRequest.getChannelId()) ||
                "".equals(offersRequest.getChannelId().trim()) || offersRequest.getChannelId().length() == 0) {
            throw new OffersRequestInvalidException("ofs004", "channel id is not valid");
        }

        if (offersRequest.getCvv().equals(null) || offersRequest.getCvv().length() > 3 ||
                "".equals(offersRequest.getCvv().trim()) || offersRequest.getCvv().length() == 0) {
            throw new OffersRequestInvalidException("ofs005", "cvv number is invalid");
        }

        if (offersRequest.getClientId().equals(null) || offersRequest.getClientId().trim().length() == 0 ||
                "".equals(offersRequest.getRequestId())) {
            throw new OffersRequestInvalidException("ofs006", "clientid is invalid");
        }

    }
}
