package com.keybank.offers.controller;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.keybank.offers.exception.BussinessException;
import com.keybank.offers.exception.OffersRequestInvalidException;
import com.keybank.offers.exception.SystemException;
import com.keybank.offers.model.OffersRequest;
import com.keybank.offers.model.OffersResponse;
import com.keybank.offers.model.StatusBlock;
import com.keybank.offers.service.IOffersDetailsService;
import com.keybank.offers.validator.OffersDetailsValidator;

@RunWith(MockitoJUnitRunner.class)
public class OfferDetailsControllerTest {
	
	
	@Mock
	OffersDetailsValidator offersDetailsValidator;
	
	@Mock
	IOffersDetailsService offerDetaisService;
	
	@InjectMocks
	OfferDetailsController offerDetailsController=new OfferDetailsController();
	
	@Test
	public void getOffers() throws SystemException, BussinessException, OffersRequestInvalidException {
		OffersResponse offerResponse=new OffersResponse();
		StatusBlock sb=new StatusBlock();
		sb.setResponseCode("200");
		sb.setResponseMessage("success");
		offerResponse.setStatusBlock(sb);
		OffersRequest request=new OffersRequest();
		when(offersDetailsValidator.validateRequest(any(OffersRequest.class))).thenReturn("sucess");
		when(offerDetaisService.getOffers(any(OffersRequest.class))).thenReturn(offerResponse);
		offerDetailsController.getOffers("WEB", "0521115004823913", "123", "Sree", "12/2022", "ONLINE", "123", "Success");
	}
	

}
