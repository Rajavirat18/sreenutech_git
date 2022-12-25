package com.keybank.offers.serviceclient;

import com.keybank.offers.model.CardDetailsRequest;
import com.keybank.offers.model.CardDetailsResponse;
import org.springframework.stereotype.Component;



@Component
public class CardDetailsServiceImpl implements  ICardDetailsServiceClient{


    public CardDetailsResponse getCardDetails(CardDetailsRequest cardDetailsRequest) {
        CardDetailsResponse cardDetailsResponse = new CardDetailsResponse();
        cardDetailsResponse.setCardProduct("abcd");
        cardDetailsResponse.setIsRenewal(String.valueOf(true));
        cardDetailsResponse.setEnrollmentDate("22-11-2022");
        cardDetailsResponse.setStatus("active");
        return cardDetailsResponse;
    }
}
