package com.keybank.offers.model;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
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
