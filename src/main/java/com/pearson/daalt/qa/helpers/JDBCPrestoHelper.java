package com.pearson.daalt.qa.helpers;


import com.facebook.presto.jdbc.internal.guava.annotations.VisibleForTesting;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by vamarra on 2/28/15.
 */
public class JDBCPrestoHelper {

    private Connection connection;
    private Statement statement;
    private String hiveDriver;
    private String connectionString;
    private ResultSet resultSet;

    static{
        try {
            Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        } catch (ClassNotFoundException e) {
            System.out.println("Presto Driver is missing");
            e.printStackTrace();
        }
    }


    public JDBCPrestoHelper(String host, String port) throws InterruptedException, SQLException {
       // String sql = "SELECT * FROM sys.node";
       // String url = "jdbc:presto://"+host+":"+port+"/hive/defualt";
        String url = "jdbc:presto://"+host+":"+port+"/hive/default?user=test&password=null&SSL=true";
        Connection connection = DriverManager.getConnection(url, "test", null);
        statement = connection.createStatement();


    }

    public JDBCPrestoHelper(String host, String port, String userId, String pwd )throws InterruptedException, SQLException{

        String url = "jdbc:presto://"+host+":"+port+"/hive/default?user=test&password=null&SSL=true";
        Connection connection = DriverManager.getConnection(url, "test".trim(), pwd);
        statement = connection.createStatement();

    }
    
    public JDBCPrestoHelper(String host, String port, String catalog,String schema,String userId, String password) {
        try {
            connection = DriverManager.getConnection("jdbc:presto://" + host + ":" + port + "/" + catalog + "/" + schema + "" ,"+ userId + " ,"");
            statement = connection.createStatement();
            
        } catch(Exception e){
            System.out.println("exception:"+e);
            }
    }



    public int getTableRowCount(String tableName) throws InterruptedException, SQLException {
        int rowCount=0;
        try {
            String tableDefinitionQuery = "select count(1)  from " + tableName;
           // Statement statement = connection.createStatement();
             resultSet = statement.executeQuery(tableDefinitionQuery);
            while (resultSet.next()) {
                rowCount= resultSet.getInt(1);
            }

        } catch (Exception e) {

            System.err.print(e);
        }
        return rowCount;
    }

    public int getTableRowCountOnQuery(String query) throws InterruptedException, SQLException {
        int rowCount=0;
        try {
           // String tableDefinitionQuery = "select count(1)  from " + tableName;
            // Statement statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                rowCount= resultSet.getInt(1);
            }

        } catch (Exception e) {

            System.err.print(e);
        }
        return rowCount;
    }

    public Map<String,String> getTableDefinition(String tableName) throws InterruptedException, SQLException {

        Map<String,String> tableDef = new HashMap<String, String>();
        try {
            String tableDefinitionQuery = "describe " + tableName;

            resultSet = statement.executeQuery(tableDefinitionQuery);
            while (resultSet.next()) {
                if(resultSet.getString(1) != null && resultSet.getString(1).length()>0 &
                        resultSet.getString(2) != null && resultSet.getString(2).length()>0)
                    tableDef.put(resultSet.getString(1).trim().toLowerCase(),resultSet.getString(2).trim().toLowerCase());
            }

        } catch (Exception e) {

            System.err.print(e);
        }
        return tableDef;
    }


    public ResultSet getTableRows(String tableName) throws InterruptedException, SQLException {

        try {
            String tableDefinitionQuery = "select * from " + tableName +" a"; // + " order by 1 ASC LIMIT 1";

            resultSet = statement.executeQuery(tableDefinitionQuery);
        } catch (Exception e) {
            System.err.print(e);


        }
        return resultSet;
    }


    public ResultSet getTableRowsOnQuery(String query)  {

        try {
            //String tableDefinitionQuery = query;
            //PreparedStatement preStatement = connection.prepareStatement(query);
              //resultSet = preStatement.executeQuery();
            String tableDefinitionQuery = query;
            resultSet = statement.executeQuery(tableDefinitionQuery);

           // resultSet = statement.executeQuery(tableDefinitionQuery);

            System.out.println("after query is executed");
        }
        catch (Exception e) {

            System.err.print(e);
        }
       /* catch (SQLException e) {

            System.err.print(e);
         }*/

        return resultSet;
    }
    
    
    public void executeQuery(String query)  {

        try {
            String tableDefinitionQuery = query;

            // boolean b = statement.execute(tableDefinitionQuery);
             
             statement.execute(tableDefinitionQuery);

            System.out.println("after query is executed");
        }
        catch (Exception e) {

            System.err.print(e);
        }
    }

    public void closeConnectionPool() {
        try {
            if (resultSet != null)
                resultSet.close();
            if (statement != null)
                statement.close();
            if (connection != null)
                connection.close();
        } catch (Exception e) {
            System.err.print(e);
        }
    }

}
