package com.keybank.offers.Service;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.keybank.offers.builder.OffersDetailsRequestBuilder;
import com.keybank.offers.builder.OffersDetailsResponseBuilder;
import com.keybank.offers.dao.IOfferDetailsDao;
import com.keybank.offers.exception.BussinessException;
import com.keybank.offers.exception.SystemException;
import com.keybank.offers.model.CardDetailsRequest;
import com.keybank.offers.model.CardDetailsResponse;
import com.keybank.offers.model.Offers;
import com.keybank.offers.model.OffersDaoResponse;
import com.keybank.offers.model.OffersRequest;
import com.keybank.offers.model.OffersResponse;
import com.keybank.offers.model.StatusBlock;
import com.keybank.offers.service.OffersDetailsServiceImpl;
import com.keybank.offers.serviceclient.ICardDetailsServiceClient;
@RunWith(MockitoJUnitRunner.class)
public class OfferDetailsServiceImplTest {
	@Mock
	IOfferDetailsDao offerDetailsDao;
	@Mock
	OffersDetailsRequestBuilder offerdetailsrequestbulider;
	@Mock
    OffersDetailsResponseBuilder offersDetailsResponseBuilder;

	@Mock
	ICardDetailsServiceClient cardDetailsServiceClient;

	@InjectMocks
	OffersDetailsServiceImpl offersdeatilsserviceimple = new OffersDetailsServiceImpl();

	@SuppressWarnings("unchecked")
	@Test
	public void getoffers() throws SystemException, BussinessException {
		OffersRequest offersrequest = new OffersRequest();
		offersrequest.setCardNum("238364872826");
		offersrequest.setChannelId("web");
		offersrequest.setClientId("online");
		offersrequest.setCvv("123");
		offersrequest.setExpDate("12-22");
		offersrequest.setMessageTimestamp("success");
		offersrequest.setNameOnCard("sreenu");
		offersrequest.setRequestId("123");
		CardDetailsRequest carddeatilsreq = new CardDetailsRequest();
		carddeatilsreq.setCardNum(offersrequest.getCardNum());
		carddeatilsreq.setCvv(offersrequest.getCvv());
		carddeatilsreq.setExpDate(offersrequest.getExpDate());
		carddeatilsreq.setNameOnCard(offersrequest.getNameOnCard());
		CardDetailsResponse cardres = new CardDetailsResponse();
		cardres.setCardProduct("mobile");
		cardres.setEnrollmentDate("12-2022");
		cardres.setIsRenewal("true");
		cardres.setLastUpdateDate("12-2022");
		cardres.setStatus("sucess");
		OffersResponse offerres=new OffersResponse();
		StatusBlock sb=new StatusBlock();
		sb.setResponseCode("200");
		sb.setResponseMessage("success");
		List<Offers> offersList = new ArrayList<Offers>();
		Offers offers=new Offers();
		offers.setCreationDate("12-2022");
		offers.setDescription("sucess");
		offers.setExpDate("12-2023");
		offers.setImageUrl("https://urlmain");
		offers.setName("raja");
		offers.setOfferId("123");
		offers.setOffersType("name");
		offers.setStock("good");
		offerres.setStatusBlock(sb);
		offerres.setOffers(offersList);
		
		when(offerdetailsrequestbulider.buildCardDetailsReqest(any(OffersRequest.class))).thenReturn(carddeatilsreq);
		when(cardDetailsServiceClient.getCardDetails(any(CardDetailsRequest.class))).thenReturn(cardres);
		when(offersDetailsResponseBuilder.buildResponsePath(any(OffersDaoResponse.class))).thenReturn(offerres);
		offersdeatilsserviceimple.getOffers(offersrequest);

	}
}
