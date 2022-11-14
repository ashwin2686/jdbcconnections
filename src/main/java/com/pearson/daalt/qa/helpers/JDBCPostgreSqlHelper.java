package com.pearson.daalt.qa.helpers;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by vamarra on 10/6/17.
 */
public class JDBCPostgreSqlHelper {

    private Connection connection;
    private Statement statement;
    private String connectionString;
    private ResultSet resultSet;


    static{
        try {
            System.out.println("Before loading the driver");
            Class.forName("org.postgresql.Driver");
            // Class.forName("com.amazon.redshift.jdbc41.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("postgresql JDBC Driver is missing");
            e.printStackTrace();
        }
    }

    public JDBCPostgreSqlHelper(String host, String port)throws InterruptedException, SQLException {

        connectionString = "jdbc:postgresql://"+host+":"+port;
        connection = DriverManager.getConnection(connectionString);
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }


    public JDBCPostgreSqlHelper(String host, String port, String userId, String password)throws InterruptedException, SQLException{
        connectionString = "jdbc:postgresql://"+host+":"+port+"/cri";
        System.out.println("connection string::"+connectionString);
        Properties props = new Properties();
        props.setProperty("user", userId);
        props.setProperty("password",password);
        connection = DriverManager.getConnection(connectionString,props);



        // connection = DriverManager.getConnection(connectionString);
       /* statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);*/
    }

    public JDBCPostgreSqlHelper(String host, String port, String dataBaseName, String userName, String pwd)throws InterruptedException, SQLException{


        connection = DriverManager.getConnection("jdbc:postgresql://"+host+":"+port+"/"+dataBaseName+"", userName,pwd);
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }


    public HashMap<String, String> getTableDefinitionByMetaData(String tableName) throws InterruptedException, SQLException {
        HashMap<String, String> columnDefinitions = new HashMap<String, String>();
        try {
            String tableDefinitionQuery = "select * from " + tableName + " limit 1;";
            resultSet = this.getTableRowsOnQuery(tableDefinitionQuery);//statement.executeQuery(tableDefinitionQuery);
            ResultSetMetaData rsmd = resultSet.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                columnDefinitions.put(rsmd.getColumnName(i),rsmd.getColumnTypeName(resultSet.findColumn(rsmd.getColumnName(i))));
            }

        } catch (Exception e) {
            System.err.print(e);
        }
        return columnDefinitions;
    }


    public Map<String,String> getTableDefinition(String tableName) throws InterruptedException, SQLException {
        Map<String,String> tableDef = new HashMap<String, String>();
        try {
            String tableDefinitionQuery = "describe  " + tableName;
            // resultSet = statement..execute(tableDefinitionQuery);
            while (resultSet.next()) {

                if(resultSet.getString(2) != null && !resultSet.getString(2).trim().equalsIgnoreCase("binary"))

                    tableDef.put(resultSet.getString("Name").trim().toLowerCase(),resultSet.getString("Type").trim().toLowerCase());
            }
        } catch (Exception e) {

            System.err.print(e);
        }
        return tableDef;
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
        JDBCRedShiftHelper helper = null;
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


    public int getTableRowCount(String tableName) throws InterruptedException, SQLException {

        int rowCount=0;
        try {
            String tableDefinitionQuery = "select count(*) as count  from " + tableName;
            PreparedStatement preStatement = connection.prepareStatement(tableDefinitionQuery);
            ResultSet resultSet = preStatement.executeQuery();
            // resultSet = statement.executeQuery(tableDefinitionQuery);
            while (resultSet.next()) {
                rowCount= resultSet.getInt(1);
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
