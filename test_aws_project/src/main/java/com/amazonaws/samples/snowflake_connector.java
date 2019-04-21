package com.amazonaws.samples;

import java.sql.*;
import java.util.Properties;

public class snowflake_connector {
	
	  public void SnowflakeUpload(String user, String password, String account, String db, String schema, String table, 
			  String SnowflakeConnectString, String s3Path, String aws_key_id, String aws_secret_key,  ResultSetMetaData output) throws Exception
	  {
	    // get connection
	    System.out.println("Create JDBC connection");
	    Connection connection = getConnection(user, password, account, db, schema, SnowflakeConnectString);
	    createDatabase(connection, db);
	    createSchema(connection, schema, db);
	    createSnowflakeTable(output, connection, table, db, schema);
	    System.out.println("Done creating JDBC connectionn");
	    // create statement
	    System.out.println("Create JDBC statement");
	    Statement statement = connection.createStatement();
	    System.out.println("Done creating JDBC statementn");
	    // create external stage out of S3 bucket
	    statement.executeUpdate("create or replace stage external_stage file_format = (type='csv') url = '" + s3Path + "'"
	    		+ "credentials = (aws_key_id='" + aws_key_id + "' aws_secret_key='"
	    				+ aws_secret_key +"');");
	    // copy data in external stage into snowflake table
	    statement.executeUpdate("copy into "+ table +" from '" + s3Path + 
	    		"' credentials = (aws_key_id='" + aws_key_id + "' aws_secret_key='" + aws_secret_key + 
	    		"')file_format = (type = csv field_delimiter = ',');");

	    statement.close();
	  }
	  
	  
	   private static Connection getConnection(String user, String password, String account, String db, String schema,
			   String SnowflakeConnectString) throws SQLException {
		   
	    try
	    {
	      Class.forName("com.snowflake.client.jdbc.SnowflakeDriver");
	    }
	    catch (ClassNotFoundException ex)
	    {
	     System.err.println("Driver not found");
	    }
	    // build connection properties
	    Properties properties = new Properties();
	    properties.put("user", user);     // with your username
	    properties.put("password", password); // with your password
	    properties.put("account", account);  // with your account name
	    //properties.put("db", db);       // with target database name
	    //properties.put("schema", schema);   // target schema name
	    //properties.put("tracing", "on");

	    // create a new connection
	    String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");
	    // use the default connection string if it is not set in environment
	    if(connectStr == null)
	    {
	     connectStr = SnowflakeConnectString; // replace accountName with your account name
	    }
	    return DriverManager.getConnection(connectStr, properties);
	  }
	   
	   private static void createSnowflakeTable(ResultSetMetaData output, Connection connection, String table,
			   String db, String scm) throws SQLException {
		    Statement statement = connection.createStatement();
		    
		    // assemble columns and column types as string
		    String schema= "";
		    for(int i = 0; i < output.getColumnCount(); i++) {
		    	schema = schema + "," + output.getColumnName(i+1) + " " + output.getColumnTypeName(i+1);
		    }
		    schema = schema.replaceFirst(",", "");
		    // set up database
		    statement.executeQuery("use " + db + ";");
		    statement.executeQuery("use schema " + scm + ";");
		    System.out.println(table);
		    schema = schema.replaceAll("UNSIGNED", "");
		    System.out.println(schema);
		    // create new table using schema
		    statement.executeUpdate("create table " + table + " (" + schema + 
		    		");");
	   }
	   
	   public static void createDatabase(Connection connection, String db) throws SQLException {
		   Statement statement = connection.createStatement();
		   statement.executeQuery("create database if not exists "+ db +";");
	   }
	   
	   public static void createSchema(Connection connection, String schema, String db) throws SQLException {
		   Statement statement = connection.createStatement();
		    statement.executeQuery("use " + db + ";");
		   statement.executeQuery("create schema if not exists "+ schema +";");
	   }
	
}
