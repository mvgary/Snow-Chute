package com.amazonaws.samples;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.amazonaws.samples.mysql_connector;


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

public class SnowChuteController {
	
	private mysql_connector mysql = null;
	private aws_connector aws = null;
	private snowflake_connector snowflake = null;
	private String tempFilePath = null;
	
	public SnowChuteController(String sqlURL, String sqlusername, String sqlpassword, String tempFilePath, String clientRegion, 
    		String bucketName, String user, String password, String account, String SnowflakeConnectString, String s3Path, String aws_key_id, String aws_secret_key) throws SQLException {
		this.tempFilePath = tempFilePath;
        this.mysql = new mysql_connector(sqlURL, sqlusername, sqlpassword, tempFilePath);
        this.aws = new aws_connector(clientRegion, bucketName);
        this.snowflake = new snowflake_connector(user, password, account,  SnowflakeConnectString, s3Path, aws_key_id, aws_secret_key);

	}
	
	    public void importTable(String sqltableName, String keyName, String db, String table,
	    		String schema) throws Exception {
	    	
	    	//read from mysql table and write results to temp file
	        ResultSet rs = this.mysql.readTable(sqltableName);
	        ResultSetMetaData md = this.mysql.getMetaData();
	        this.mysql.writeToFile(rs, this.tempFilePath);
	        
	        //upload data from local to aws
	        this.aws.S3Upload(keyName, this.tempFilePath);
	         
	        //upload data from S3 into Snowflake
	        this.snowflake.SnowflakeUpload(db, schema, table, md);
	        
	        //close out any connectors
	        this.mysql.close();
	        this.snowflake.close();
	        System.out.println("program complete");
	    }
	    
	    /*
	    public static void importTable2(String sqlURL, String sqltableName, String sqlusername, String sqlpassword, String clientRegion, 
	    		String bucketName, String keyName, String user, String password, String account, String db, String table,
	    		String schema, String SnowflakeConnectString, String s3Path, String aws_key_id, String aws_secret_key) throws Exception {
	    	
	    	 jdbc_connector dao = new jdbc_connector();
		        //read data from mysql database
		        //String localFilePath = dao.getFileLocation(sqltableName, sqlURL, sqlusername, sqlpassword);
		        String localFilePath = "C:\\Users\\micha\\Downloads";
		        ResultSetMetaData output = dao.readDataBase(sqltableName, sqlURL, sqlusername, sqlpassword, localFilePath);
	    }
	*/
}
