package com.keybank.offers.controller;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.keybank.offers.exception.BussinessException;
import com.keybank.offers.exception.OffersRequestInvalidException;
import com.keybank.offers.exception.SystemException;
import com.keybank.offers.model.OffersRequest;
import com.keybank.offers.model.OffersResponse;
import com.keybank.offers.service.IOffersDetailsService;
import com.keybank.offers.validator.OffersDetailsValidator;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;


@RestController
@RequestMapping("/v1")
@Tag(name = "offers details controller")
public class OfferDetailsController {
	@Autowired
	private OffersDetailsValidator offersDetailsValidator;
	@Autowired
	private IOffersDetailsService offersDetailsService;
	Logger logger=LogManager.getLogger(OfferDetailsController.class);

	@Operation(summary = "get offer detaisl")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = {
					@Content(mediaType = "applicatio/json", schema = @Schema(implementation = OffersResponse.class)) }),

			@ApiResponse(responseCode = "400", description = "Bad Request"),
			@ApiResponse(responseCode = "500", description = "Internal Server Error"),
			@ApiResponse(responseCode = "404", description = "Not Found") })
	@GetMapping("/offers/{cardNum}/{cvv}/{nameOnCard}/{expDate}")
	public OffersResponse getOffers(@RequestHeader("client_id") String clientId,
			@PathVariable("cardNum") String cardNum, @PathVariable("cvv") String cvv,
			@PathVariable("nameOnCard") String nameOnCard, @PathVariable("expDate") String expDate,
			@RequestHeader("channel_id") String channelId, @RequestHeader("request_id") String reuqestId,
			@RequestHeader("message_ts") String messageTimeStamp)
	
	
			throws OffersRequestInvalidException, SystemException, BussinessException {
		
		logger.info("Inside getOffers method");
		
		
		OffersRequest offersRequest = new OffersRequest();

		offersRequest.setCvv(cvv);
		offersRequest.setCardNum(cardNum);
		offersRequest.setExpDate(expDate);
		offersRequest.setNameOnCard(nameOnCard);
		offersRequest.setRequestId(reuqestId);
		offersRequest.setChannelId(channelId);
		offersRequest.setClientId(clientId);
		offersRequest.setMessageTimestamp(messageTimeStamp);
		
		logger.info("Inside getOffers method--->"+ offersRequest);
		offersDetailsValidator.validateRequest(offersRequest);

		OffersResponse offersResponse = offersDetailsService.getOffers(offersRequest);
		return offersResponse;

	}
}
