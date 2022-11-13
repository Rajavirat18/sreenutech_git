package com.example;

import java.util.stream.IntStream;

import javax.swing.text.MaskFormatter;

public class Creditcard {
	public static void main(String[] args) {

		String creditcardid = "123456789170092";
		if (creditcardid.length() == 16) {
			MaskifyusingIntstream(creditcardid);
		} else {
			throw new InvalidCreditCardnumberException("invalid credit card number");
		}

	}

	private static void MaskifyusingIntstream(String creditcardid) {
		String lastcharacters = creditcardid.substring(creditcardid.length() - 12);
		StringBuilder sb = new StringBuilder();
		IntStream.rangeClosed(0, creditcardid.length() - lastcharacters.length() - 1).forEach(num -> sb.append('*'));

		String val = "4"; // use 4 here to insert spaces every 4 characters
		String result = lastcharacters.replaceAll("(.{" + val + "})", "$1 ").trim();
		sb.append(" " + result);
		System.out.println(sb);

		char someChar = '0';
		if (creditcardid.charAt(11) == someChar && creditcardid.charAt(12) == someChar) {
			System.out.println("Credit card is primary card");
		} else {
			System.out.println("Credit card is not a primary card");
		}

	}

}
