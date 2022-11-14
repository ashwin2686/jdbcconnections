package com.pearson.daalt.qa.tests;

import java.sql.*;
import java.util.Enumeration;
import java.util.List;

/**
 * Created by vamarra on 3/15/16.
 */
public class HiveJDBCSample {


    public static void main(String [] args){


        System.out.println("it is working");

        System.out.println("begining");
        //Class.forName("org.apache.hive.jdbc.HiveDriver");
        try {
            // Class.forName("com.amazon.hive.jdbc3.HS2Driver");
            // Class.forName("org.apache.hive.jdbc4.HiveDriver");
            // Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
            System.out.println("Before loading driver .");
            Class.forName("com.amazon.hive.jdbc4.HS2Driver");
            // com.amazon.hive.jdbc4.HS2Driver
            // Class.forName("org.apache.hive.jdbc.HiveDriver");
            System.out.println("After loading driver .");




        }catch(Exception e){
            System.out.println("error::"+e);

        }

        Connection cnct= null;
        ResultSet rset = null;
        try {

            //jdbc:hive2://10.201.1.40:10002/default;UID=hadoop;pwd=
            System.out.println("Driver list.");
            Enumeration<Driver> drivers = DriverManager.getDrivers();
            while(drivers.hasMoreElements()){

                System.out.println(drivers.nextElement().toString());
            }

            //
            System.out.println("Before one");
            cnct = DriverManager.getConnection("jdbc:hive2://10.201.1.37:10000/default", "hadoop", "");
            System.out.println("After one");
            Statement stmt = cnct.createStatement();
            String queryStr="select count(*) from subpub_messages where dt='2016-02-14'";
            System.out.println("Before executing query.");

            rset = stmt.executeQuery(queryStr);
            System.out.println("After executing query.");

            while(rset.next()){
                System.out.println(rset.getInt(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if(cnct != null)
                    cnct.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        System.out.println("End of the program");
    }

}

