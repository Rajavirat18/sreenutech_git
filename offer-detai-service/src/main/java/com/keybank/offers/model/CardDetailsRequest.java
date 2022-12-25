package com.keybank.offers.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CardDetailsRequest {

    private String cardNum;
    private String cvv;
    private String nameOnCard;
    private String expDate;

}
