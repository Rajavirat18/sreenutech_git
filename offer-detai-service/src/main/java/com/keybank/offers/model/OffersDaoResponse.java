package com.keybank.offers.model;

import lombok.Data;

import java.util.List;
@Data
public class OffersDaoResponse {

    private String responeCode;
    private String responseMessage;
    private List<OffersDao> offersDao;
}
