package com.keybank.offers.service;

import com.keybank.offers.builder.OffersDetailsRequestBuilder;
import com.keybank.offers.builder.OffersDetailsResponseBuilder;
import com.keybank.offers.dao.IOfferDetailsDao;
import com.keybank.offers.exception.BussinessException;
import com.keybank.offers.exception.SystemException;
import com.keybank.offers.model.*;
import com.keybank.offers.serviceclient.ICardDetailsServiceClient;
import com.keybank.offers.util.OffersDetailsUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Service
public class OffersDetailsServiceImpl implements IOffersDetailsService {

    @Autowired
    private IOfferDetailsDao offerDetailsDao;

    @Autowired
    private ICardDetailsServiceClient cardDetailsServiceClient;

    @Autowired
    private OffersDetailsRequestBuilder offersDetailsRequestBuilder;

    @Autowired
    private OffersDetailsResponseBuilder offersDetailsResponseBuilder;

    public OffersResponse getOffers(OffersRequest offersRequest) throws SystemException, BussinessException {

        OffersResponse offersResponse = null;

        CardDetailsRequest cardDetailsRequest = offersDetailsRequestBuilder.buildCardDetailsReqest(offersRequest);

        CardDetailsResponse cardDetailsResponse = cardDetailsServiceClient.getCardDetails(cardDetailsRequest);

        Boolean enrollStatus = OffersDetailsUtil.comparisiononeyearDiff(cardDetailsResponse.getEnrollmentDate());
        Boolean cardStatus = OffersDetailsUtil.checkCardStatus(cardDetailsResponse.getStatus());

        if (enrollStatus == true && cardStatus == true) {

            OffersDaoRequest offersDaoRequest = offersDetailsRequestBuilder.buildOffersDaoRequest(offersRequest);

            OffersDaoResponse offersDaoResponse = offerDetailsDao.getOffers(offersDaoRequest);

            offersResponse = offersDetailsResponseBuilder.buildResponsePath(offersDaoResponse);
        } 
        return offersResponse;
    }
}
