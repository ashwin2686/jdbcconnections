package com.pearson.daalt.qa.helpers;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by vamarra on 1/28/15.
 */
public class JDBCSqlServerHelper {

    private Connection connection;
    private Statement statement;
    private String hiveDriver;
    private String connectionString;
    private ResultSet resultSet;


    static{
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            System.out.println("Hive JDBC Driver is missing");
            e.printStackTrace();
        }
    }

    public JDBCSqlServerHelper(String host, String port)throws InterruptedException, SQLException {

        connectionString = "jdbc:sqlserver://"+host+":"+port;
        connection = DriverManager.getConnection(connectionString);
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }


    public JDBCSqlServerHelper(String host, String port, String userId, String password)throws InterruptedException, SQLException{
        connectionString = "jdbc:sqlserver://"+host+":"+port+"/default,"+userId+","+password+"";
        connection = DriverManager.getConnection(connectionString);
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }

    public JDBCSqlServerHelper(String host, String port, String dataBaseName,String userName, String pwd)throws InterruptedException, SQLException{


        connection = DriverManager.getConnection("jdbc:sqlserver://"+host+":"+port+";DatabaseName="+dataBaseName+"", userName,pwd);
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }


    public Map<String,String> getTableDefinition(String tableName) throws InterruptedException, SQLException {
        Map<String,String> tableDef = new HashMap<String, String>();
        try {
            String tableDefinitionQuery = "exec sp_columns " + tableName;
            resultSet = statement.executeQuery(tableDefinitionQuery);
            while (resultSet.next()) {

                if(resultSet.getString(2) != null && !resultSet.getString(2).trim().equalsIgnoreCase("binary"))

                tableDef.put(resultSet.getString("COLUMN_NAME").trim().toLowerCase(),resultSet.getString("TYPE_NAME").trim().toLowerCase());
            }
        } catch (Exception e) {

            System.err.print(e);
        }
        return tableDef;
    }

    public ArrayList getTableList() throws InterruptedException, SQLException {

        ArrayList<String> tableList = new ArrayList<String>();
        try {
            String tableDefinitionQuery = "show tables";
            resultSet = statement.executeQuery(tableDefinitionQuery);
            while (resultSet.next()) {
                tableList.add(resultSet.getString(1));
            }
        } catch (Exception e) {

            System.err.print(e);
        }
        return tableList;
    }

    public ArrayList getTableList(String schemaName) throws InterruptedException, SQLException {

        ArrayList<String> tableList = new ArrayList<String>();
        try {
            String tableDefinitionQuery = "SELECT t.name \n" +
                    "  FROM sys.tables AS t\n" +
                    "  INNER JOIN sys.schemas AS s\n" +
                    "  ON t.[schema_id] = s.[schema_id]\n" +
                    "  WHERE s.name = N'"+schemaName+"';";
            resultSet = statement.executeQuery(tableDefinitionQuery);
            while (resultSet.next()) {
                tableList.add(resultSet.getString(1));
            }
        } catch (Exception e) {

            System.err.print(e);
        }
        return tableList;
    }


    public ResultSet getTableRows(String tableName) throws InterruptedException, SQLException {

        try {
            String tableDefinitionQuery = "select TOP 10 * from " + tableName; //+ " order by 1 ASC";
            resultSet = statement.executeQuery(tableDefinitionQuery);
        } catch (Exception e) {

            System.err.print(e);
        }
        return resultSet;
    }

    public ResultSet getTableRowsOnQuery(String query) throws InterruptedException, SQLException {

        try {
            String tableDefinitionQuery = query;
            resultSet = statement.executeQuery(tableDefinitionQuery);
        } catch (Exception e) {

            System.err.print(e);
        }
        return resultSet;
    }

    public String getColumnValueOnQuery(String query) {
        ResultSet msSQLresultSet = null;
        JDBCSqlServerHelper helper = null;
        String columnValue = null;
        try {
            //helper = new JDBCHelper("10.52.124.22", "1433", "XL_prod_global", "michaeld", "md-89hj");
            //msSQLresultSet = helper.getTableRowsOnQuery("select count(*) from users where user_id = 67529 and authsystem_id = 1");
            msSQLresultSet = statement.executeQuery(query);
            int index = 1;
            while (msSQLresultSet.next()) {
                columnValue = msSQLresultSet.getString(index);
            }


        } catch (Exception e) {
            System.err.print(e);
        }

        return columnValue;
    }


    public int getTableRowCount(String tableName) throws InterruptedException, SQLException {

        int rowCount=0;
        try {
            String tableDefinitionQuery = "select count(*) as count  from " + tableName;
            resultSet = statement.executeQuery(tableDefinitionQuery);
            while (resultSet.next()) {
                rowCount= resultSet.getInt(1);
            }

        } catch (Exception e) {

            System.err.print(e);
        }
        return rowCount;
    }
    
    
    public long getTableRowCountBig(String tableName) throws InterruptedException, SQLException {

        long rowCount=0;
        try {
            String tableDefinitionQuery = "select COUNT_BIG(*) as count  from " + tableName;
            resultSet = statement.executeQuery(tableDefinitionQuery);
            while (resultSet.next()) {
                rowCount= resultSet.getLong(1);
            }

        } catch (Exception e) {

            System.err.print(e);
        }
        return rowCount;
    }

    public boolean findTableExists(String tableName)
    {

        boolean tableExists=false;
        try {
            String tableDefinitionQuery = "if object_id('"+tableName+"') is not null select existence=1 else select existence=0";
            resultSet = statement.executeQuery(tableDefinitionQuery);
            while (resultSet.next()) {
              if(resultSet.getInt(1) == 1)
                  tableExists = true;
              else
                  tableExists = false;
            }
        } catch (Exception e) {

            System.err.print(e);
        }
        return tableExists;

    }

    public void closeConnectionPool()
    {
        try{
            if(resultSet != null)
                resultSet.close();
            if(statement != null)
                statement.close();
            if(connection != null)
                connection.close();
        }catch(Exception e){
            System.err.print(e);
        }
    }


}
