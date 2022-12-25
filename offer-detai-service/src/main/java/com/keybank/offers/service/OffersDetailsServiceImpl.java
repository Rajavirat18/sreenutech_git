package com.keybank.offers.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.keybank.offers.builder.OffersDetailsRequestBuilder;
import com.keybank.offers.builder.OffersDetailsResponseBuilder;
import com.keybank.offers.controller.OfferDetailsController;
import com.keybank.offers.dao.IOfferDetailsDao;
import com.keybank.offers.exception.BussinessException;
import com.keybank.offers.exception.SystemException;
import com.keybank.offers.model.CardDetailsRequest;
import com.keybank.offers.model.CardDetailsResponse;
import com.keybank.offers.model.OffersDaoRequest;
import com.keybank.offers.model.OffersDaoResponse;
import com.keybank.offers.model.OffersRequest;
import com.keybank.offers.model.OffersResponse;
import com.keybank.offers.serviceclient.ICardDetailsServiceClient;
import com.keybank.offers.util.OffersDetailsUtil;

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
    
	Logger logger=LogManager.getLogger(OfferDetailsController.class);


    public OffersResponse getOffers(OffersRequest offersRequest) throws SystemException, BussinessException {

    	logger.info("OffersDetailsServiceImpl: getOffers()"+offersRequest);
    	
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
