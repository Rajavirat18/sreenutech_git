package com.keybank.offers.util;

import org.springframework.jmx.export.MBeanExportOperations;

public enum OffersDetailsErrorCodesEnum {

    CLIENT_ID("100","CLient id is invalid","Data Error"),
    CHANNEL_ID("101","channel id is invalid","Data Error"),
    CARD_NUMBER("102","card number invalid","Data Error"),
    CVV_NUMBER("103","cvv is invalid","Data Error"),
    DATABASE_TIMEOUT("111","database timeout","System Error"),
    DATABASE_DOWN("222","Databse is down","System Error"),
    DATABASE_SQL_ERROR("333","SQL Error","System Error");

    private String errorCode;
    private String errorMessage;
    private String typeOfError;

    OffersDetailsErrorCodesEnum(String errorCode,String errorMessage,String typeOfError){
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.typeOfError = typeOfError;
    }

    public static Boolean checkErrorCode(String dbErrorCode,String typeOfError){
        for (OffersDetailsErrorCodesEnum   offersEnum :OffersDetailsErrorCodesEnum.values()) {
            if(dbErrorCode.equals(offersEnum.errorCode) && typeOfError.equals(offersEnum.typeOfError)){
                return  true;
            }
        }
        return false;
    }

}
