package com.keybank.offers.model;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class OffersRequest {

    private String cardNum;
    private String cvv;
    private String expDate;
    private String nameOnCard;
    private String clientId;
    private String channelId;
    private String requestId;
    private String messageTimestamp;

}
