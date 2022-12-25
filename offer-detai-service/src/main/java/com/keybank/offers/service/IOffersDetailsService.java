package com.keybank.offers.service;

import org.springframework.stereotype.Component;

import com.keybank.offers.exception.BussinessException;
import com.keybank.offers.exception.SystemException;
import com.keybank.offers.model.OffersDaoRequest;
import com.keybank.offers.model.OffersDaoResponse;
import com.keybank.offers.model.OffersRequest;
import com.keybank.offers.model.OffersResponse;

@Component
public interface IOffersDetailsService {

    public OffersResponse getOffers(OffersRequest offersRequest) throws SystemException, BussinessException;

}
