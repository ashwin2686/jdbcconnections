package com.pearson.daalt.qa.helpers;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by vamarra on 1/28/15.
 */
public class JDBCOracleHelper {

    private Connection connection;
    private Statement statement;
    private String connectionString;
    private ResultSet resultSet;


    static{
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            System.out.println("Oracle JDBC Driver is missing");
            e.printStackTrace();
        }
    }

    public JDBCOracleHelper(String host, String port)throws InterruptedException, SQLException {

        connectionString = "jdbc:oracle:thin:@"+host+":"+port;
        connection = DriverManager.getConnection(connectionString);
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }


    public JDBCOracleHelper(String host, String port, String userId, String password)throws InterruptedException, SQLException{
        connectionString = "jdbc:oracle:thin:@"+host+":"+port+":orcl";
        System.out.println("connection string::"+connectionString);
        Properties props = new Properties();
        props.setProperty("user", userId);
        props.setProperty("password",password);
        connection = DriverManager.getConnection(connectionString,props);



       // connection = DriverManager.getConnection(connectionString);
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }

    public JDBCOracleHelper(String host, String port, String dataBaseName, String userId, String password)throws InterruptedException, SQLException{

        connectionString = "jdbc:oracle:thin:@"+host+":"+port+"/"+dataBaseName;
        System.out.println("connection string::"+connectionString);
        Properties props = new Properties();
        props.setProperty("user", userId);
        props.setProperty("password",password);
        connection = DriverManager.getConnection(connectionString,props);
        // connection = DriverManager.getConnection(connectionString);
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

    }


    public Map<String,String> getTableDefinitionBySchema(String schema, String tableName) throws InterruptedException, SQLException {
        Map<String,String> tableDef = new HashMap<String, String>();
        try {

            String tableDefinitionQuery="\n" +
                    "select * from dba_tab_columns " +
                    "where owner='"+schema.toUpperCase()+"' " +
                    "AND table_name='"+tableName.toUpperCase()+"' ";

            //String tableDefinitionQuery = "desc  " + tableName;
            resultSet = statement.executeQuery(tableDefinitionQuery);
            while (resultSet.next()) {

                if(resultSet.getString(2) != null && !resultSet.getString(2).trim().equalsIgnoreCase("binary"))

                tableDef.put(resultSet.getString("COLUMN_NAME").trim().toLowerCase(),resultSet.getString("DATA_TYPE").trim().toLowerCase());
            }
        } catch (Exception e) {

            System.err.print(e);
        }
        return tableDef;
    }


    public HashMap<String, String> getTableDefinition(String tableName) throws InterruptedException, SQLException {
        HashMap<String, String> columnDefinitions = new HashMap<String, String>();
        try {
            String tableDefinitionQuery = "select * from " + tableName + " where ROWNUM<2";
            resultSet = statement.executeQuery(tableDefinitionQuery);
            ResultSetMetaData rsmd = resultSet.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                columnDefinitions.put(rsmd.getColumnName(i),rsmd.getColumnTypeName(resultSet.findColumn(rsmd.getColumnName(i))));
            }

        } catch (Exception e) {
            System.err.print(e);
        }
        return columnDefinitions;
    }




    public ArrayList getTableList() throws InterruptedException, SQLException {

        ArrayList<String> tableList = new ArrayList<String>();
        try {
            String tableDefinitionQuery = "select * from tab";
           // String tableDefinitionQuery = "select * from ASSESSMENT_MV WHERE rownum <= 5";

            PreparedStatement preStatement = connection.prepareStatement(tableDefinitionQuery);
            ResultSet resultSet = preStatement.executeQuery();
           // resultSet = statement.executeQuery(tableDefinitionQuery);
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
            String tableDefinitionQuery = "select * from " + tableName; //+ " order by 1 ASC";
            PreparedStatement preStatement = connection.prepareStatement(tableDefinitionQuery);
            ResultSet resultSet = preStatement.executeQuery();

           // resultSet = statement.executeQuery(tableDefinitionQuery);
        } catch (Exception e) {

            System.err.print(e);
        }
        return resultSet;
    }

    public ResultSet getTableRowsOnQuery(String query) throws InterruptedException, SQLException {

        ResultSet resultSet =null;
        try {
            String tableDefinitionQuery = query;
            PreparedStatement preStatement = connection.prepareStatement(tableDefinitionQuery);
             resultSet = preStatement.executeQuery();

            // resultSet = statement.executeQuery(tableDefinitionQuery);
        } catch (Exception e) {

            System.err.print(e);
        }
        return resultSet;
    }

    public String getColumnValueOnQuery(String query) {
        ResultSet msSQLresultSet = null;
        JDBCOracleHelper helper = null;
        String columnValue = null;
        try {
            //helper = new JDBCHelper("10.52.124.22", "1433", "XL_prod_global", "michaeld", "md-89hj");
            //msSQLresultSet = helper.getTableRowsOnQuery("select count(*) from users where user_id = 67529 and authsystem_id = 1");
           // msSQLresultSet = statement.executeQuery(query);
            PreparedStatement preStatement = connection.prepareStatement(query);
            ResultSet resultSet = preStatement.executeQuery();

            int index = 1;
            while (msSQLresultSet.next()) {
                columnValue = msSQLresultSet.getString(index);
            }


        } catch (Exception e) {
            System.err.print(e);
        }

        return columnValue;
    }


    public long getTableRowCount(String tableName) throws InterruptedException, SQLException {

        long rowCount=0;
        try {
            String tableDefinitionQuery = "select count(*) as count  from " + tableName;
            PreparedStatement preStatement = connection.prepareStatement(tableDefinitionQuery);
            ResultSet resultSet = preStatement.executeQuery();
           // resultSet = statement.executeQuery(tableDefinitionQuery);
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
