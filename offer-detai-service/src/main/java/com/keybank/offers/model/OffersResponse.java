package com.keybank.offers.model;

import lombok.Data;

import java.util.List;

@Data
public class OffersResponse {

    private StatusBlock statusBlock;
    private List<Offers> offers;
}
