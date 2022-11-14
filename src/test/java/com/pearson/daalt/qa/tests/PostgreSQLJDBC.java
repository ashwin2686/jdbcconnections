package com.pearson.daalt.qa.tests;

import java.sql.*;

/**
 * Created by vamarra on 10/6/17.
 */
public class PostgreSQLJDBC {
    public static void main( String args[] ) {
        Connection c = null;
        Statement stmt = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://use1a-daalt-mbr-cri-db-rds-prd-01.cqpdxjaydk1j.us-east-1.rds.amazonaws.com:5432/cri",
                            "il_root", "$Trong_password1");
            System.out.println("Opened database successfully");

            stmt = c.createStatement();
            String sql = "select * from cri_user.user_starts_self_assessment limit 1";
            PreparedStatement preStatement = c.prepareStatement(sql);
            ResultSet resultSet = preStatement.executeQuery();

            int index = 1;
            while (resultSet.next()) {
                System.out.println("query details:"+resultSet.getString(1));
            }




            stmt.close();
            c.close();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
        System.out.println("Table created successfully");
    }



}
