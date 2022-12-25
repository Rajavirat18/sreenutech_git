package com.keybank.offers.model;

import lombok.Data;

@Data
public class CardDetailsResponse {
    private String enrollmentDate;
    private String status;
    private String lastUpdateDate;
    private String isRenewal;
    private String cardProduct;
}
