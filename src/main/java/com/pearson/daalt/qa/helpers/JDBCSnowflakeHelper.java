package com.pearson.daalt.qa.helpers;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Udasaas on 07/08/19.
 */
public class JDBCSnowflakeHelper {
    private Connection connection;
    private Statement statement;
    private String connectionString;
    private ResultSet resultSet;

   

    public JDBCSnowflakeHelper(String userId, String password) throws InterruptedException, SQLException {
       // this.connectionString = "jdbc:snowflake://pearsonproduct.us-east-1.snowflakecomputing.com/?";
        
        this.connectionString = "jdbc:snowflake://pearsonproduct.us-east-1.privatelink.snowflakecomputing.com/?";
        System.out.println("connection string::" + this.connectionString);
        Properties props = new Properties();
        props.setProperty("user", userId);
        props.setProperty("password", password);
         this.connection = DriverManager.getConnection(this.connectionString, props);
    }

    public JDBCSnowflakeHelper(String userId, String password,String warehouse,String role) throws InterruptedException, SQLException {
        // this.connectionString = "jdbc:snowflake://pearsonproduct.us-east-1.snowflakecomputing.com/?";

        this.connectionString = "jdbc:snowflake://pearsonproduct.us-east-1.privatelink.snowflakecomputing.com/?";
        System.out.println("connection string::" + this.connectionString);
        Properties props = new Properties();
        props.setProperty("user", userId);
        props.setProperty("password", password);
        props.setProperty("warehouse", warehouse);
        props.setProperty("role", role);
        this.connection = DriverManager.getConnection(this.connectionString, props);
    }



    

  

    public HashMap<String, String> getTableDefinitionByMetaData(String tableName) throws InterruptedException, SQLException {
        HashMap columnDefinitions = new HashMap();

        try {
            String e = "select * from " + tableName + " limit 1;";
            this.resultSet = this.getTableRowsOnQuery(e);
            ResultSetMetaData rsmd = this.resultSet.getMetaData();

            for(int i = 1; i <= rsmd.getColumnCount(); ++i) {
                columnDefinitions.put(rsmd.getColumnName(i), rsmd.getColumnTypeName(this.resultSet.findColumn(rsmd.getColumnName(i))));
            }
        } catch (Exception var6) {
            System.err.print(var6);
        }

        return columnDefinitions;
    }

    public Map<String, String> getTableDefinition(String tableName) throws InterruptedException, SQLException {
        HashMap tableDef = new HashMap();

        try {
            (new StringBuilder()).append("describe  ").append(tableName).toString();

            while(this.resultSet.next()) {
                if(this.resultSet.getString(2) != null && !this.resultSet.getString(2).trim().equalsIgnoreCase("binary")) {
                    tableDef.put(this.resultSet.getString("Name").trim().toLowerCase(), this.resultSet.getString("Type").trim().toLowerCase());
                }
            }
        } catch (Exception var4) {
            System.err.print(var4);
        }

        return tableDef;
    }

    public ArrayList getTableList() throws InterruptedException, SQLException {
        ArrayList tableList = new ArrayList();

        try {
            String e = "select * from tab";
            PreparedStatement preStatement = this.connection.prepareStatement(e);
            ResultSet resultSet = preStatement.executeQuery();

            while(resultSet.next()) {
                tableList.add(resultSet.getString(1));
            }
        } catch (Exception var5) {
            System.err.print(var5);
        }

        return tableList;
    }

    public ResultSet getTableRows(String tableName) throws InterruptedException, SQLException {
        try {
            String e = "select * from " + tableName;
            PreparedStatement preStatement = this.connection.prepareStatement(e);
            ResultSet resultSet = preStatement.executeQuery();
        } catch (Exception var5) {
            System.err.print(var5);
        }

        return this.resultSet;
    }

    public boolean updateRow(String query) throws InterruptedException, SQLException {
        boolean updateFlag=false;
        try {
           // String e = "select * from " + tableName;
            PreparedStatement preStatement = this.connection.prepareStatement(query);
            boolean insertFlag=preStatement.execute();
        } catch (Exception var5) {
            System.err.print(var5);
        }

        return updateFlag;

    }

    public ResultSet getTableRowsOnQuery(String query) throws InterruptedException, SQLException {
        ResultSet resultSet = null;

        try {
            PreparedStatement preStatement = this.connection.prepareStatement(query);
            resultSet = preStatement.executeQuery();
        } catch (Exception var5) {
            System.err.print(var5);
        }

        return resultSet;
    }

    public String getColumnValueOnQuery(String query) {
        Object msSQLresultSet = null;
        Object helper = null;
        String columnValue = null;

        try {
            PreparedStatement e = this.connection.prepareStatement(query);
            ResultSet resultSet = e.executeQuery();

            for(byte index = 1; ((ResultSet)msSQLresultSet).next(); columnValue = ((ResultSet)msSQLresultSet).getString(index)) {
                ;
            }
        } catch (Exception var8) {
            System.err.print(var8);
        }

        return columnValue;
    }

    public int getTableRowCount(String tableName) throws InterruptedException, SQLException {
        int rowCount = 0;

        try {
            String e = "select count(*) as COUNT  from " + tableName;
            PreparedStatement preStatement = this.connection.prepareStatement(e);

            for(ResultSet resultSet = preStatement.executeQuery(); resultSet.next(); rowCount = resultSet.getInt(1)) {
                ;
            }
        } catch (Exception var6) {
            System.err.print(var6);
        }

        return rowCount;
    }

    public void insertRow(String query) throws InterruptedException, SQLException {
        try {
            PreparedStatement e = this.connection.prepareStatement(query);
            boolean insertFlag = e.execute();
        } catch (Exception var4) {
            System.err.print(var4);
        }

    }
    
    
    public void insertrowsofArraylist(ArrayList<List<String>>list,int rowsize,String query) throws InterruptedException, SQLException {
        try {
        	int i =0;
            PreparedStatement e = this.connection.prepareStatement(query);
            while(i<rowsize){
         	   
        	    for (int j = 0; j < list.size(); j++) {
        	    	e.setString(j+1, list.get(j).get(i));
        	       
        	    }
        	    e.addBatch();
        	    i++;
        	    }
        	    
        	    e.executeBatch();
        	    e.close();
        } catch (Exception var4) {
            System.err.print(var4);
        }

    }

    public boolean findTableExists(String tableName) {
        boolean tableExists = false;

        try {
            String e = "if object_id(\'" + tableName + "\') is not null select existence=1 else select existence=0";
            this.resultSet = this.statement.executeQuery(e);

            while(this.resultSet.next()) {
                if(this.resultSet.getInt(1) == 1) {
                    tableExists = true;
                } else {
                    tableExists = false;
                }
            }
        } catch (Exception var4) {
            System.err.print(var4);
        }

        return tableExists;
    }

    public void closeConnectionPool() {
        try {
            if(this.resultSet != null) {
                this.resultSet.close();
            }

            if(this.statement != null) {
                this.statement.close();
            }

            if(this.connection != null) {
                this.connection.close();
            }
        } catch (Exception var2) {
            System.err.print(var2);
        }

    }

    static {
        try {
            System.out.println("Before loading the driver");
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
        } catch (ClassNotFoundException var1) {
            System.out.println("Snowflake JDBC Driver is missing");
            var1.printStackTrace();
        }

    }
}
