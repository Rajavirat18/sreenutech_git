package com.keybank.offers.model;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class CardDetailsResponse {
    private String enrollmentDate;
    private String status;
    private String lastUpdateDate;
    private String isRenewal;
    private String cardProduct;
}
