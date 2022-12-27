package com.keybank.offers.model;

import java.util.List;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class OffersDaoResponse {

    private String responeCode;
    private String responseMessage;
    private List<OffersDao> offersDao;
}
