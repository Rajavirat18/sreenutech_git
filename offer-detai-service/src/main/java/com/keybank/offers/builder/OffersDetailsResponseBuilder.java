package com.keybank.offers.builder;

import com.keybank.offers.model.*;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component

public class OffersDetailsResponseBuilder {

	public OffersResponse buildResponsePath(OffersDaoResponse offersDaoResponse) {
		StatusBlock statusBlock = new StatusBlock();
		statusBlock.setResponseCode(offersDaoResponse.getResponeCode());
		statusBlock.setResponseMessage(offersDaoResponse.getResponseMessage());

		List<Offers> offersList = new ArrayList<Offers>();

		// List<OffersDao> offersDao = offersDaoResponse.getOffersDao();
		for (OffersDao offersDao : offersDaoResponse.getOffersDao()) {
			Offers offers = new Offers();
			offers.setName(offersDao.getName());
			offers.setOffersType(offersDao.getOffersType());
			offers.setOfferId(offersDao.getOfferId());
			offers.setExpDate(offersDao.getExpDate());
			offers.setDescription(offersDao.getDescription());
			offersList.add(offers);

		}

		OffersResponse offersResponse = new OffersResponse();
		offersResponse.setStatusBlock(statusBlock);
		offersResponse.setOffers(offersList);
		return offersResponse;
	}
}
