package com.amazonaws.samples;

import java.sql.*;
import java.util.Properties;

public class snowflake_connector {
	
	private Connection connection = null;
	private String user = null;
	private String password = null;
	private String account = null;
	private String SnowflakeConnectString = null;
	private String s3Path = null;
	private String aws_key_id = null;
	private String aws_secret_key = null;
	private Statement statement = null;
	
	public snowflake_connector(String user, String password, String account, 
			String SnowflakeConnectString, String s3Path, String aws_key_id, String aws_secret_key) throws SQLException {
	    
		this.user = user;
		this.password = password;
		this.account = account;
		this.SnowflakeConnectString = SnowflakeConnectString;
		this.s3Path = s3Path;
		this.aws_key_id = aws_key_id;
		this.aws_secret_key = aws_secret_key;
		this.connection = getConnection();
		this.statement = connection.createStatement();
		
	}
	
	  public void SnowflakeUpload(String db, String schema, String table, ResultSetMetaData output) throws Exception
	  {
	    // make sure db and schema are created
	    System.out.println("Creating db and schema");
	    this.createDatabase(db);
	    this.createSchema(schema, db);
	    this.createSnowflakeTable(output, table, db, schema);
	    System.out.println("Done creating db and schema");
	    // create statement
	    System.out.println("Creating external stage at: " + this.s3Path);
	    // create external stage out of S3 bucket
	    this.statement.executeUpdate("create or replace stage external_stage file_format = (type='csv') url = '" + this.s3Path + "'"
	    		+ "credentials = (aws_key_id='" + this.aws_key_id + "' aws_secret_key='"
	    				+ aws_secret_key +"');");
	    System.out.println("Done creating stage table at: " + this.s3Path);
	    System.out.println("Copying into table");
	    // copy data in external stage into snowflake table
	    this.statement.executeUpdate("copy into "+ table +" from '" + this.s3Path + 
	    		"' credentials = (aws_key_id='" + this.aws_key_id + "' aws_secret_key='" + this.aws_secret_key + 
	    		"')file_format = (type = csv field_delimiter = ',');");
	    System.out.println("Done copying into table");
	  }
	  
	  
	   private Connection getConnection() throws SQLException {
		   
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
	    properties.put("user", this.user);     // with your username
	    properties.put("password", this.password); // with your password
	    properties.put("account", this.account);  // with your account name
	    //properties.put("db", db);       // with target database name
	    //properties.put("schema", schema);   // target schema name
	    //properties.put("tracing", "on");

	    // create a new connection
	    String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");
	    // use the default connection string if it is not set in environment
	    if(connectStr == null)
	    {
	     connectStr = this.SnowflakeConnectString; // replace accountName with your account name
	    }
	    return DriverManager.getConnection(connectStr, properties);
	  }
	   
	   public void createSnowflakeTable(ResultSetMetaData output, String table,
			   String db, String scm) throws SQLException {
		    
		    // assemble columns and column types as string
		    String schema= "";
		    for(int i = 0; i < output.getColumnCount(); i++) {
		    	schema = schema + "," + output.getColumnName(i+1) + " " + output.getColumnTypeName(i+1);
		    }
		    schema = schema.replaceFirst(",", "");
		    // point to correct db and schema
		    this.statement.executeQuery("use " + db + ";");
		    this.statement.executeQuery("use schema " + scm + ";");
		    schema = schema.replaceAll("UNSIGNED", "");
		    // create new table using schema
		    this.statement.executeUpdate("create table " + table + " (" + schema + 
		    		");");
	   }
	   
	   public void createDatabase(String db) throws SQLException {
		   this.statement.executeQuery("create database if not exists "+ db +";");
	   }
	   
	   public void createSchema(String schema, String db) throws SQLException {
		   this.statement.executeQuery("use " + db + ";");
		   this.statement.executeQuery("create schema if not exists "+ schema +";");
	   }
	
	   public void close() throws SQLException {
		   if(this.connection != null) {
			   this.connection.close();
		   }
		   if(this.statement != null) {
			   this.statement.close();
		   }
	   }
}
