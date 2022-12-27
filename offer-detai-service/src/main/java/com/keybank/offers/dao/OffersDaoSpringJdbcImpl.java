/* Copyright @ 2022, Keybank pvt ltd. All Rights are reserved. You should not disclose the
 * information outside, otherwise terms and conditions will apply 
 * 
 */
package com.keybank.offers.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlOutParameter;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.SqlReturnResultSet;
import org.springframework.jdbc.object.StoredProcedure;
import org.springframework.stereotype.Component;

import com.keybank.offers.exception.BussinessException;
import com.keybank.offers.exception.SystemException;
import com.keybank.offers.model.OffersDao;
import com.keybank.offers.model.OffersDaoRequest;
import com.keybank.offers.model.OffersDaoResponse;
import com.keybank.offers.util.OffersDetailsConstant;
import com.keybank.offers.util.OffersDetailsErrorCodesEnum;

/**
 * @author sreen at 08-Dec-2022 Descritpion:
 */
@Component
@Primary
public class OffersDaoSpringJdbcImpl extends StoredProcedure implements RowMapper, IOfferDetailsDao {

	@Autowired
	public OffersDaoSpringJdbcImpl(JdbcTemplate jdbcTemplate) {

		super(jdbcTemplate, OffersDetailsConstant.STORED_PROCEDURE_NAME);
		compileStoredProcedureParam();

	}

	private void compileStoredProcedureParam() {

		// register input params
		declareParameter(new SqlParameter(OffersDetailsConstant.CLIENT_ID, Types.VARCHAR));
		declareParameter(new SqlParameter(OffersDetailsConstant.CHANNEL_ID, Types.VARCHAR));
		declareParameter(new SqlParameter(OffersDetailsConstant.CARDNUMBER, Types.VARCHAR));
		declareParameter(new SqlParameter(OffersDetailsConstant.CVV, Types.VARCHAR));
		declareParameter(new SqlParameter(OffersDetailsConstant.NAME_ON_CARD, Types.VARCHAR));
		declareParameter(new SqlParameter(OffersDetailsConstant.EXP_DATE, Types.VARCHAR));

		// register output params
		declareParameter(new SqlOutParameter(OffersDetailsConstant.RESP_CODE_OUT, Types.VARCHAR));
		declareParameter(new SqlOutParameter(OffersDetailsConstant.RESP_MSG_OUT, Types.VARCHAR));

		// register Resultset

		declareParameter(new SqlReturnResultSet(OffersDetailsConstant.OFFERS_RESULT_SET_NAME, this));

		compile();

	}

	
	public OffersDaoResponse getOffers(OffersDaoRequest offerDaoRequest) throws BussinessException, SystemException {

		System.out.println("spring jdbc offers dao getOffers -- start");

		OffersDaoResponse daoResp = new OffersDaoResponse();

		// prepare the sp input
		Map<String, Object> spInput = new HashMap<String, Object>();
		System.out.println("client id is : " + offerDaoRequest.getClientId());
		spInput.put(OffersDetailsConstant.CLIENT_ID, offerDaoRequest.getClientId());
		spInput.put(OffersDetailsConstant.CHANNEL_ID, offerDaoRequest.getChannelId());
		spInput.put(OffersDetailsConstant.CARDNUMBER, offerDaoRequest.getCardNum());
		spInput.put(OffersDetailsConstant.CVV, offerDaoRequest.getCvv());
		spInput.put(OffersDetailsConstant.NAME_ON_CARD, offerDaoRequest.getNameOnCard());
		spInput.put(OffersDetailsConstant.EXP_DATE, offerDaoRequest.getExpDate());

		// invoke stored procedure
		Map<String, Object> spOutput = super.execute(spInput);
		System.out.println("spOutput :" + spOutput);
		String dbResponseCode = null;
		String dbResponseMsg = null;

		if (spOutput != null && spOutput.get(OffersDetailsConstant.RESP_CODE_OUT) != null
				&& spOutput.get(OffersDetailsConstant.RESP_MSG_OUT) != null) {

			dbResponseCode = spOutput.get(OffersDetailsConstant.RESP_CODE_OUT).toString();
			dbResponseMsg = spOutput.get(OffersDetailsConstant.RESP_MSG_OUT).toString();
			System.out.println("dbResponseCode :" + dbResponseCode);
			System.out.println("dbResponseMsg :" + dbResponseMsg);
		}

		List<OffersDao> offersDao = new ArrayList<OffersDao>();

		// handling database response
		if (OffersDetailsConstant.SUCCESS_RESP_CODE.contentEquals(dbResponseCode)) {

			// Prepare the dao resposne with the help of resultset

			offersDao = (List<OffersDao>) spOutput.get("offersResult");
			daoResp.setOffersDao(offersDao);
			daoResp.setResponeCode(dbResponseCode);
			daoResp.setResponseMessage(dbResponseMsg);

		} else if (OffersDetailsErrorCodesEnum.checkErrorCode(dbResponseCode, OffersDetailsConstant.DATA_ERROR)) {

			throw new BussinessException(dbResponseCode, dbResponseMsg);
		} else if (OffersDetailsErrorCodesEnum.checkErrorCode(dbResponseCode, OffersDetailsConstant.SYSTEM_ERROR)) {
			throw new SystemException(dbResponseCode, dbResponseMsg);
		} else {
			throw new SystemException(OffersDetailsConstant.GENERIC_RESP_CODE, OffersDetailsConstant.GENERIC_RESP_MSG);
		}

		System.out.println("offers dao getOffers -- exit");
		return daoResp;
	}

	public OffersDao mapRow(ResultSet rs, int rowNum) throws SQLException {

		System.out.println("entered into mapRow");

		OffersDao offersDao = new OffersDao();

		offersDao.setOfferId(rs.getString(OffersDetailsConstant.OFFER_ID));
		offersDao.setName(rs.getString(OffersDetailsConstant.OFFER_NAME));
		offersDao.setOffersCode(rs.getString(OffersDetailsConstant.OFFER_CODE));
		offersDao.setDesc(rs.getString(OffersDetailsConstant.OFFER_DESC));
		offersDao.setExpDate(rs.getString(OffersDetailsConstant.OFFER_EXPIRY_DATE));
		offersDao.setCreationDate(rs.getString(OffersDetailsConstant.OFFER_CREATION_DATE));
		offersDao.setImageUrl(rs.getString(OffersDetailsConstant.OFFER_IMAGE_URL));
		offersDao.setStock(rs.getString(OffersDetailsConstant.OFFER_STOCK));
		offersDao.setOffersType(rs.getString(OffersDetailsConstant.OFFER_TYPE));

		return offersDao;
	}

}
