package com.pearson.daalt.qa.helpers;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by vamarra on 1/28/15.
 *
 *         JDBCHiveHelper class to provide connections to Hive
 *         and methods to query the Database
 *
 *
 */


public class JDBCHiveHelper {

    private Connection connection;
    private Statement statement;
    private String hiveDriver;
    private String connectionString;
    private ResultSet resultSet;
    String addJar = "ADD JAR /home/hadoop/hive/auxlib/json-serde-1.3-jar-with-dependencies.jar";


    static{
       try {

          /*
           Class.forName("org.apache.hive.jdbc.HiveDriver");
           Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
           Class.forName("com.amazon.hive.jdbc3.HS2Driver");
          */
           Class.forName("com.amazon.hive.jdbc41.HS2Driver");

        //   Class.forName("org.apache.hive.jdbc4.HiveDriver");
          // Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");  //- Quoble driver


        } catch (Exception e) {
            System.out.println("Hive JDBC Driver is missing");
            e.printStackTrace();
        }
    }

    public JDBCHiveHelper(String host, String port)throws InterruptedException, SQLException {

        connectionString = "jdbc:hive2://"+host+":"+port+"/default";
        connection = DriverManager.getConnection(connectionString);
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }


    public JDBCHiveHelper(String host, String port, String userId, String password)throws InterruptedException, SQLException{
        try {
            // connectionString = "jdbc:hive2://"+host+":"+port+"/default,"+userId;
            connection = DriverManager.getConnection("jdbc:hive2://"+host+":"+port+"/default", ""+userId+"", "");
           // statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
             //     ResultSet.CONCUR_READ_ONLY);
            statement=connection.createStatement();

        }catch(Exception e){

            e.printStackTrace();
            System.err.print(e);

        }
    }

    public JDBCHiveHelper(String host, String port, String schema,String userId, String password) {
        try {
            connectionString = "jdbc:hive2://" + host + ":" + port + "/" + schema + ";UID=" + userId + ";pwd=" + password + "";
            // System.out.println("connection string::"+connectionString);
            connection = DriverManager.getConnection("jdbc:hive2://" + host + ":" + port + "/" + schema, "" + userId + "", "");
             /*statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);*/
            statement = connection.createStatement();
            statement.execute("use "+schema);
        } catch(Exception e){
            System.out.println("exception:"+e);
            }
    }



    public Map<String,String> getTableDefinition(String tableName) throws InterruptedException, SQLException {

        Map<String,String> tableDef = new HashMap<String, String>();
        try {
            String tableDefinitionQuery = "describe " + tableName;
            statement.execute(addJar);
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
            statement.execute(addJar);
            resultSet = statement.executeQuery(tableDefinitionQuery);
        } catch (Exception e) {
            System.err.print(e);


        }
        return resultSet;
    }


    public ResultSet getTableRowsOnQuery(String query)  {

        try {
            String tableDefinitionQuery = query;

           // boolean b = statement.execute(tableDefinitionQuery);
            statement.execute(addJar);
            resultSet = statement.executeQuery(tableDefinitionQuery);

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
             statement.execute(addJar);
             statement.execute(tableDefinitionQuery);

            System.out.println("after query is executed");
        }
        catch (Exception e) {

            System.err.print(e);
        }
    }




    public ArrayList getTableList() throws InterruptedException, SQLException {

        ArrayList<String> tableList = new ArrayList<String>();


        try {
            String tableDefinitionQuery = "show tables";
            statement.execute(addJar);
            resultSet = statement.executeQuery(tableDefinitionQuery);
            while (resultSet.next()) {
                tableList.add(resultSet.getString(1));
            }
        } catch (Exception e) {

            System.err.print(e);
        }
        return tableList;
    }




    public int getTableRowCount(String tableName) throws InterruptedException, SQLException {

        int rowCount=0;
        try {
            String tableDefinitionQuery = "select count(1)  from " + tableName;
            statement.execute(addJar);
            resultSet = statement.executeQuery(tableDefinitionQuery);
            while (resultSet.next()) {
                rowCount= resultSet.getInt(1);
            }

        } catch (Exception e) {

            System.err.print(e);
        }
        return rowCount;
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
