package com.keybank.offers.serviceclient;

import org.springframework.stereotype.Component;

import com.keybank.offers.model.CardDetailsRequest;
import com.keybank.offers.model.CardDetailsResponse;

@Component
public interface ICardDetailsServiceClient {

    public CardDetailsResponse getCardDetails(CardDetailsRequest  cardDetailsRequest);
}
