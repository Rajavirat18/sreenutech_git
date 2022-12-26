package com.keybank.offers.model;

import java.util.List;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class OffersResponse {

    private StatusBlock statusBlock;
    private List<Offers> offers;
}
