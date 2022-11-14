package com.pearson.daalt.qa.helpers;


import com.datastax.driver.core.*;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.lang.System.out;



/**
 * Created by vamarra on 2/27/15.
 */


public class JDBCCassandraHelper {

   private Session session = null;
   private Cluster cluster = null;
   private ResultSet resultSet = null;


    /**
     * This constructor initialize the cassandra connectivity object
     *
     * @param host
     * @param port
     * @param keySpace
     */
    public JDBCCassandraHelper(String host, int port, String keySpace){

     try {

         SocketOptions options = new SocketOptions();
         options.setConnectTimeoutMillis(10000);
         options.setReadTimeoutMillis(10000);

          cluster = Cluster.builder().addContactPoints(host).withPort(port).withSocketOptions(options).withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                 .withReconnectionPolicy(new ConstantReconnectionPolicy(1000L)).build();

         Metadata metadata = cluster.getMetadata();
         System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());

          session = cluster.connect(keySpace);
     }catch(Exception e){
         e.printStackTrace();
     }

    }


    /**
     * This method gets table content
     * @param tableName
     */

    public ResultSet getTableRows(String tableName) throws InterruptedException, SQLException {

        try {
            String query = "select * from " + tableName;
            resultSet = session.execute(query);
        } catch (Exception e) {

            System.err.print(e);
        }
        return resultSet;
    }

    /**
     * This method gets table content based on the query provided
     * @param query
     */
    public ResultSet getTableRowsOnQuery(String query) throws InterruptedException, SQLException {

        try {
            String tableDefinitionQuery = query;
            resultSet = session.execute(tableDefinitionQuery);
        } catch (Exception e) {

            System.err.print(e);
        }
        return resultSet;
    }

    /**
     * This method gets table count provided table name
     * @param tableName
     */
    public int getTableRowCount(String tableName) throws InterruptedException, SQLException {

        int rowCount=0;
        try {
            String tableDefinitionQuery = "select count(*) as count  from " + tableName;
            resultSet = session.execute(tableDefinitionQuery);
            for (Row row: resultSet) {
                rowCount= row.getInt(1);
            }

        } catch (Exception e) {

            System.err.print(e);
        }
        return rowCount;
    }

    /**
     * This method closes all the cassandra connectiions
     *
     */
    public void closeConnectionPool()
    {
        try{

            if(session != null)
                session.close();
            if(cluster != null)
                cluster.close();
        }catch(Exception e){
            System.err.print(e);
        }
    }




}


