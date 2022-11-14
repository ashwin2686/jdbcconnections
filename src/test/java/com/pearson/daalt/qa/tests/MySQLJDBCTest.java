package com.pearson.daalt.qa.tests;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import com.pearson.daalt.qa.helpers.JDBCMySqlHelper;

/**
 * Created by vthiygo on 11/9/17.
 */
public class MySQLJDBCTest{

	public static void main( String args[] ) {
        
        try {
        	
            JDBCMySqlHelper JDBCMySqlHelpersample = new JDBCMySqlHelper("use1a-daalt-mbr-cardinal-db-rds-stg-01.cluster-cqpdxjaydk1j.us-east-1.rds.amazonaws.com", "3306",
    				"cardinal_user", "cardinal_root", "$Trong_password1");
            
            System.out.println("Opened database successfully");
            String query = "select * from cardinal_user.ale_overall_mastery limit 1";
             
            ResultSet rs=JDBCMySqlHelpersample.getTableRowsOnQuery(query);  
            while(rs.next()) { 
            	System.out.println("query details: "+rs.getString(1));
            	}             
            System.out.println("Closing the connection");
            }catch(Exception e){ 
            	System.err.println( e.getClass().getName()+": "+ e.getMessage() );
                System.exit(0);
            	}                        
    }
}
