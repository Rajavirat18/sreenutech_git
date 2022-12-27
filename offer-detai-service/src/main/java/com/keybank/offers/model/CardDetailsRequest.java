package com.keybank.offers.model;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class CardDetailsRequest {

    private String cardNum;
    private String cvv;
    private String nameOnCard;
    private String expDate;

}
