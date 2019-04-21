package com.amazonaws.samples;
import java.sql.ResultSetMetaData;

import com.amazonaws.samples.jdbc_connector;

public class test_class {
	
	    public static void importTable(String sqlURL, String sqltableName, String sqlusername, String sqlpassword, String clientRegion, 
	    		String bucketName, String keyName, String user, String password, String account, String db, String table,
	    		String schema, String SnowflakeConnectString, String s3Path, String aws_key_id, String aws_secret_key) throws Exception {
	    	/*
	    	//initialize mysql jdbc connector variables
	    	String sqlURL = "jdbc:mysql://localhost:3306/sales";
	    	String sqltableName = "product";
	    	String sqlusername = "root";
	    	String sqlpassword = "bigmike";
	    	//initialize aws jdbc connector variables
	    	String clientRegion = "us-west-1";
	        String bucketName = "mvgary-test-bucket-123494";
	        String keyName = "test_key1";
	        //initialize Snowflake jdbc connector variables
	        String user = "mvgary094";
	        String password = "Bigmike094";
	        String account = "ot24432";
	        String db = "salesDB";
	        String table = "product2";
	        String schema = "salesSchema";
	        String SnowflakeConnectString = "jdbc:snowflake://ot24432.snowflakecomputing.com/";
	        String s3Path = "s3://mvgary-test-bucket-123494/test_key1";
		    String aws_key_id = "AKIAIBXWU6SVPE6TRESQ";
		    String aws_secret_key = "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";
	        */
	    	//create new jdbc connector
	        jdbc_connector dao = new jdbc_connector();
	        //read data from mysql database
	        //String localFilePath = dao.getFileLocation(sqltableName, sqlURL, sqlusername, sqlpassword);
	        String localFilePath = "C:\\Users\\micha\\Downloads\\outfile.csv";
	        ResultSetMetaData output = dao.readDataBase(sqltableName, sqlURL, sqlusername, sqlpassword, localFilePath);
	        
	        //create new aws connector
	        aws_connector aws_connection = new aws_connector();
	        //upload data from local to aws
	        aws_connection.S3Upload(clientRegion, bucketName, keyName, localFilePath);
	        
	        //create new snowflake connector 
	        snowflake_connector sf = new snowflake_connector();
	        //upload data from S3 into Snowflake
	        sf.SnowflakeUpload(user, password, account, db, schema, table, SnowflakeConnectString, s3Path, aws_key_id, aws_secret_key, output);
	        
	        System.out.println("program complete");
	    }
	    
	    
	    public static void importTable2(String sqlURL, String sqltableName, String sqlusername, String sqlpassword, String clientRegion, 
	    		String bucketName, String keyName, String user, String password, String account, String db, String table,
	    		String schema, String SnowflakeConnectString, String s3Path, String aws_key_id, String aws_secret_key) throws Exception {
	    	
	    	 jdbc_connector dao = new jdbc_connector();
		        //read data from mysql database
		        //String localFilePath = dao.getFileLocation(sqltableName, sqlURL, sqlusername, sqlpassword);
		        String localFilePath = "C:\\Users\\micha\\Downloads";
		        ResultSetMetaData output = dao.readDataBase(sqltableName, sqlURL, sqlusername, sqlpassword, localFilePath);
	    }
	
}
