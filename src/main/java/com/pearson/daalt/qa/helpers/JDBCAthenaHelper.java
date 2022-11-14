/**
 * 
 */
package com.pearson.daalt.qa.helpers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * @author udasaas
 *
 */
public class JDBCAthenaHelper {

	private Connection connection;
	private Statement statement;
	private ResultSet resultSet;

	//public JDBCAthenaHelper(String AWSAccessKey, String AWSSecretAccessKey, String s3stagingdir)
	public JDBCAthenaHelper(String s3stagingdir){
		try {

			Properties info = new Properties();
			//info.put("user", AWSAccessKey);
			//info.put("password", AWSSecretAccessKey);
			//info.put("s3_staging_dir", s3stagingdir);
			
			info.setProperty("s3_staging_dir", s3stagingdir);
			info.put("aws_credentials_provider_class",
					"com.amazonaws.auth.InstanceProfileCredentialsProvider");

			Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");
			
			//Class.forName("com.simba.athena.jdbc.Driver");
			
			

			Connection connection = DriverManager.getConnection("jdbc:awsathena://athena.us-east-1.amazonaws.com:443/",
					info);
			
			System.out.println("Athena connection is"+connection);
			
			statement = connection.createStatement();
			
			System.out.println("Athena statement is"+statement);

		} catch (Exception e) {
			System.out.println("Athena JDBC Driver is missing");
			e.printStackTrace();
		}
	}

	public HashMap<String, String> getTableDefinition(String tableName) throws InterruptedException, SQLException {
		HashMap<String, String> columnDefinitions = new HashMap<String, String>();
		try {
			String tableDefinitionQuery = "select * from " + tableName + " limit 1;";
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

	public Map<String,String> getTableDefinitionByDescribe(String tableName) throws InterruptedException, SQLException {

		Map<String,String> tableDef = new HashMap<String, String>();
		try {
			//String tableDefinitionQuery = "describe " + tableName;
			
			String tableDefinitionQuery = "describe " + tableName;
			
			System.out.println("tableDefinitionQuery is "+tableDefinitionQuery);

			resultSet = statement.executeQuery(tableDefinitionQuery);
			
			
			while (resultSet.next()) {
				if(resultSet.getString(1) != null && resultSet.getString(1).length()>0) {
					String[] rowOutput = resultSet.getString(1).split("\t");

					/*System.out.println("rowOutput 0 is "+rowOutput[0]); 
					System.out.println("before array exception");
					System.out.println("rowOutput 1 is "+rowOutput[1]); 
					System.out.println("after array exception");*/
					tableDef.put(rowOutput[0].trim().toLowerCase(), rowOutput[1].trim().toLowerCase());
				}
			}

		} 
		catch(SQLException se){
		      //Handle errors for JDBC
			System.out.println("Handle errors for JDBC");
		      se.printStackTrace();
		}
		catch (Exception e) {

			System.err.print(e);
		}
		return tableDef;
	}

	public ArrayList<String> getTableList(String databaseName) throws InterruptedException, SQLException {

		ArrayList<String> tableList = new ArrayList<String>();

		try {
			String tableDefinitionQuery = "show tables in " + databaseName;

			resultSet = statement.executeQuery(tableDefinitionQuery);
			while (resultSet.next()) {
				tableList.add(resultSet.getString("tab_name"));
			}
		} catch (Exception e) {

			System.err.print(e);
		}
		return tableList;
	}

	public ResultSet getTableRows(String tableName) throws InterruptedException, SQLException {

		try {
			String tableDefinitionQuery = "select * from " + tableName + " ";

			resultSet = statement.executeQuery(tableDefinitionQuery);
		} catch (Exception e) {
			System.err.print(e);

		}
		return resultSet;
	}

	public ResultSet executeQuery(String query) {

		String tableDefinitionQuery = query;

		try {

			System.out.println("Before the Athena query is executed");
			resultSet = statement.executeQuery(tableDefinitionQuery);

			System.out.println("after the Athena query is executed");
		} 
		
		catch(SQLException se){
		      //Handle errors for JDBC
			System.out.println("Handle errors for JDBC");
		      se.printStackTrace();
		}
		catch (Exception e) {

			System.err.print(e);
		}
		return resultSet;

	}
	
	public Boolean addpartitionQuery(String tableName , String dt) {

		
		Boolean flag = false;

		try {

			System.out.println("Before the addpartition Athena query is executed");
			flag = statement.execute("ALTER TABLE  " + tableName
					+ "  ADD IF NOT EXISTS PARTITION (dt='" + dt + "')");

			System.out.println("after the addpartition Athena query is executed");
		} 
		
		catch(SQLException se){
		      //Handle errors for JDBC
			System.out.println("Handle errors for JDBC");
		      se.printStackTrace();
		}
		catch (Exception e) {

			System.err.print(e);
		}
		return flag;

	}

	public int executeCountQuery(String query) {

		String tableDefinitionQuery = query;

		int rowCount = 0;

		try {

			resultSet = statement.executeQuery(tableDefinitionQuery);

			while (resultSet != null && resultSet.next()) {

				rowCount = resultSet.getInt("count");
			}

			System.out.println("after query is executed");
		} catch (Exception e) {

			System.err.print(e);
		}
		return rowCount;

	}

	public int getTableRowCount(String tableName) throws InterruptedException, SQLException {

		int rowCount = 0;
		try {
			String tableDefinitionQuery = "select count(1)  from " + tableName;

			resultSet = statement.executeQuery(tableDefinitionQuery);
			while (resultSet.next()) {
				rowCount = resultSet.getInt(1);
			}

		} catch (Exception e) {

			System.err.print(e);
		}
		return rowCount;
	}
	
	
	public long getTableRowCountlong(String tableName) throws InterruptedException, SQLException {

		long rowCount = 0;
		try {
			String tableDefinitionQuery = "select count(1)  from " + tableName;

			resultSet = statement.executeQuery(tableDefinitionQuery);
			while (resultSet.next()) {
				rowCount = resultSet.getLong(1);
			}

		} catch (Exception e) {

			System.err.print(e);
		}
		return rowCount;
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
