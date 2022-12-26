package com.keybank.offers.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OffersDaoRequest {
    private String cardNum;
    private String cvv;
    private String nameOnCard;
    private String expDate;
    private String clientId;
    private String channelId;
}
