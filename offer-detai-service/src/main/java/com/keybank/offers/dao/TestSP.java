package com.keybank.offers.dao;

import lombok.SneakyThrows;

import java.sql.*;

public class TestSP {
    @SneakyThrows
    public static void main(String[] args) throws ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");

        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/coms", "root", "root");

        String sql = "CALL GET_OFFER_DETAILS_V1(?,?,?,?,?,?,?,?)";

        CallableStatement cstmt = connection.prepareCall(sql);

        cstmt.setString(1, "WEB");
        cstmt.setString(2, "ONLINE");
        cstmt.setString(3, "0521115004823913");
        cstmt.setString(4, "123");
        cstmt.setString(5, "SREENU");
        cstmt.setString(6, "12/2022");

        cstmt.registerOutParameter(7, Types.VARCHAR);
        cstmt.registerOutParameter(8, Types.VARCHAR);

        boolean b = cstmt.execute();

        ResultSet rs = cstmt.executeQuery();

        // get the out params data

        String dbrespCode = cstmt.getString(7);
        String dbrespMsg = cstmt.getString(8);

        System.out.println("dbrespCode is :" + dbrespCode + ":::" + "dbrespMsg:" + dbrespMsg);

        while (rs.next()) {

            System.out.println(rs.getString(1));
            System.out.println(rs.getString(2));
            System.out.println(rs.getString(3));
            System.out.println(rs.getString(4));
            System.out.println(rs.getString(5));
            System.out.println(rs.getString(6));
            System.out.println(rs.getString(7));
            System.out.println(rs.getString(8));
            System.out.println(rs.getString(9));

        }

    }
}
