package com.keybank.offers.dao;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Repository;

import com.keybank.offers.exception.BussinessException;
import com.keybank.offers.exception.SystemException;
import com.keybank.offers.model.OffersDao;
import com.keybank.offers.model.OffersDaoRequest;
import com.keybank.offers.model.OffersDaoResponse;
import com.keybank.offers.util.OffersDetailsConstant;
import com.keybank.offers.util.OffersDetailsErrorCodesEnum;

@Repository
public class OfferDetailsDaoImpl implements IOfferDetailsDao {
	
	private static final Logger logger = LogManager.getLogger(OfferDetailsDaoImpl.class);



	public OffersDaoResponse getOffers(OffersDaoRequest offerDaoRequest) throws BussinessException, SystemException {
		
		logger.info("OfferDetailsDaoImpl: getOffers()"+offerDaoRequest);
		OffersDaoResponse offersDaoResponse = null;
		ArrayList<OffersDao> offersDaoArrayList = new ArrayList<OffersDao>();
		try {

			Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/coms", "root", "root");

			String sql = "CALL GET_OFFER_DETAILS_V1(?,?,?,?,?,?,?,?)";

			CallableStatement cstmt = connection.prepareCall(sql);

			cstmt.setString(1, "WEB");
			cstmt.setString(2, "ONLINE");
			cstmt.setString(3, offerDaoRequest.getCardNum());
			cstmt.setString(4, offerDaoRequest.getCvv());
			cstmt.setString(5, offerDaoRequest.getNameOnCard());
			cstmt.setString(6, offerDaoRequest.getExpDate());

			cstmt.registerOutParameter(7, Types.VARCHAR);
			cstmt.registerOutParameter(8, Types.VARCHAR);

			cstmt.execute();

			ResultSet rs = cstmt.executeQuery();

			// get the out params data

			String dbResponseCode = cstmt.getString(7);
			String dbResponseMessage = cstmt.getString(8);


			logger.debug("dbrespCode is :" + dbResponseCode + ":::" + "dbrespMsg:" + dbResponseMessage);
			
			if (OffersDetailsConstant.SUCCESS_RESP_CODE.equals(dbResponseCode)) {

				offersDaoResponse = new OffersDaoResponse();

				offersDaoResponse.setResponeCode(dbResponseCode);
				offersDaoResponse.setResponseMessage(dbResponseMessage);

				while (rs.next()) {

					OffersDao offersDao = new OffersDao();
					offersDao.setOfferId(rs.getString(1));
					offersDao.setOffersType(rs.getString(2));
					offersDao.setImageUrl(rs.getString(3));
					offersDao.setCreationDate(rs.getString(4));
					offersDao.setDescription(rs.getString(5));
					offersDao.setExpDate(rs.getString(6));
					offersDao.setName(rs.getString(7));
					offersDao.setStock(rs.getString(8));
					offersDao.setOffersCode(rs.getString(9));

					offersDaoArrayList.add(offersDao);
				}
				offersDaoResponse.setOffersDao(offersDaoArrayList);

			} else if (OffersDetailsErrorCodesEnum.checkErrorCode(dbResponseCode, "Data Error")) {
				logger.error("data error occured");
				throw new BussinessException(dbResponseCode, dbResponseMessage);
			} else if (OffersDetailsErrorCodesEnum.checkErrorCode(dbResponseCode, "System Error")) {
				throw new SystemException(dbResponseCode, dbResponseMessage);
			} else {
				throw new SystemException(OffersDetailsConstant.GENERIC_RESP_CODE,
						OffersDetailsConstant.GENERIC_RESP_MSG);
			}
		} catch (BussinessException bussinessException) {
			logger.error("Exception Occured whie executing the stored procedure");
			bussinessException.printStackTrace();
			throw bussinessException;
		} catch (SystemException systemException) {
			logger.error("Exception Occured whie executing the stored procedure");
			systemException.printStackTrace();
			throw systemException;
		} catch (SQLException e) {
			logger.error("Exception Occured whie executing the stored procedure");
			e.getMessage();
		}

		return offersDaoResponse;
	}
}
