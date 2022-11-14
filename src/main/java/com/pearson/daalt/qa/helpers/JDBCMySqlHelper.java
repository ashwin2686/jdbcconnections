package com.pearson.daalt.qa.helpers;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by vthiygo on 11/9/17.
 */
public class JDBCMySqlHelper {

	private Connection connection;
	private Statement statement;
	private String connectionString;
	private ResultSet resultSet;

	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("MySQL JDBC Driver is missing");
			e.printStackTrace();
		}
	}

	public JDBCMySqlHelper(String host, String port) throws InterruptedException, SQLException {

		connectionString = "jdbc:mysql://" + host + ":" + port;
		connection = DriverManager.getConnection(connectionString);
		statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	}

	public JDBCMySqlHelper(String host, String port, String userId, String password)
			throws InterruptedException, SQLException {
		connectionString = "jdbc:mysql://" + host + ":" + port + "/default," + userId + "," + password + "";
		connection = DriverManager.getConnection(connectionString);
		statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	}

	public JDBCMySqlHelper(String host, String port, String dataBaseName, String userName, String pwd)
			throws InterruptedException, SQLException {

		connection = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + dataBaseName, userName,
				pwd);
		statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	}

	public Map<String, String> getTableDefinition(String tableName) throws InterruptedException, SQLException {
		Map<String, String> tableDef = new HashMap<String, String>();
		try {
			String tableDefinitionQuery = "DESCRIBE " + tableName;
			resultSet = statement.executeQuery(tableDefinitionQuery);
			while (resultSet.next()) {
				tableDef.put(resultSet.getString("FIELD").trim().toLowerCase(),
						resultSet.getString("TYPE").trim().toLowerCase());
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

	public ResultSet getTableRows(String tableName) throws InterruptedException, SQLException {

		try {
			String tableQuery = "select * from " + tableName; // + " order by 1
																// ASC";
			PreparedStatement preStatement = connection.prepareStatement(tableQuery);
			resultSet = preStatement.executeQuery();

		} catch (Exception e) {

			System.err.print(e);
		}
		return resultSet;
	}

	public ResultSet getTableRowsOnQuery(String query) throws InterruptedException, SQLException {

		try {
			String tableQuery = query;
			resultSet = statement.executeQuery(tableQuery);
		} catch (Exception e) {

			System.err.print(e);
		}
		return resultSet;
	}

	public int getTableRowCount(String tableName) throws InterruptedException, SQLException {

		int rowCount = 0;
		try {
			String tableDefinitionQuery = "select count(*) as count  from " + tableName;
			resultSet = statement.executeQuery(tableDefinitionQuery);
			while (resultSet.next()) {
				rowCount = resultSet.getInt(1);
			}

		} catch (Exception e) {

			System.err.print(e);
		}
		return rowCount;
	}

	public boolean findTableExists(String tableName) {

		boolean tableExists = false;
		try {
			String tableDefinitionQuery = "select count(*) as count from " + tableName;
			if (statement.executeQuery(tableDefinitionQuery).first()) {
				tableExists = true;
			} else {
				tableExists = false;
			}
		} catch (Exception e) {

			System.err.print(e);
		}
		return tableExists;

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
