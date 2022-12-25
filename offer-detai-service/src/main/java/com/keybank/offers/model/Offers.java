package com.keybank.offers.model;

import lombok.Data;

@Data
public class Offers {
    private String offerId;
    private String name;
    private String description;
    private String expDate;
    private String imageUrl;
    private String creationDate;
    private String stock;
    private String offersType;

}
