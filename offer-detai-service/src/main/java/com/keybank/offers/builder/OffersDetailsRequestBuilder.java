package com.keybank.offers.builder;

import com.keybank.offers.model.CardDetailsRequest;
import com.keybank.offers.model.OffersDaoRequest;
import com.keybank.offers.model.OffersRequest;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class OffersDetailsRequestBuilder {

    public CardDetailsRequest buildCardDetailsReqest(OffersRequest offersRequest) {
        CardDetailsRequest cardDetailsRequest = new CardDetailsRequest();
        cardDetailsRequest.setCardNum(offersRequest.getCardNum());
        cardDetailsRequest.setCvv(offersRequest.getCvv());
        cardDetailsRequest.setNameOnCard(offersRequest.getNameOnCard());
        cardDetailsRequest.setExpDate(offersRequest.getExpDate());
        return cardDetailsRequest;
    }

    public OffersDaoRequest buildOffersDaoRequest(OffersRequest offersRequest) {

        OffersDaoRequest offersDaoRequest = new OffersDaoRequest();
        offersDaoRequest.setCvv(offersRequest.getCvv());
        offersDaoRequest.setExpDate(offersRequest.getExpDate());
        offersDaoRequest.setCardNum(offersRequest.getCardNum());
        offersDaoRequest.setNameOnCard(offersRequest.getNameOnCard());
        offersDaoRequest.setChannelId(offersRequest.getChannelId());
        offersDaoRequest.setClientId(offersRequest.getClientId());
        return offersDaoRequest;
    }
}
