package com.keybank.offers.dao;

import org.springframework.stereotype.Component;

import com.keybank.offers.exception.BussinessException;
import com.keybank.offers.exception.SystemException;
import com.keybank.offers.model.OffersDaoRequest;
import com.keybank.offers.model.OffersDaoResponse;

@Component
public interface IOfferDetailsDao {

    public OffersDaoResponse getOffers(OffersDaoRequest offerDaoRequest) throws BussinessException, SystemException;



}
