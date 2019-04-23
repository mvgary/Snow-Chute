package com.amazonaws.samples;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.*;

public class mysql_connector {
	private Connection connect = null;
    private Statement statement = null;
    private ResultSet resultSet = null;
    private ResultSetMetaData metaData = null;
    private String username = null;
    private String password = null;
    private String url = null;
    private String tempFilePath = null;
   
    
    public mysql_connector(String URL, String uname, String pword, String filePath) {
    	this.username = uname;
    	this.password = pword;
    	this.url = URL;
    	this.tempFilePath = filePath;
    	String credentials = "user=" + this.username + "&" + "password=" + this.password; 
    	String connectString = URL + "?" + credentials;
    	
    	try {
			Class.forName("com.mysql.jdbc.Driver");
			// Setup the connection with the DB
			this.connect = DriverManager.getConnection(connectString);
			// 	create and execute statement
			this.statement = this.connect.createStatement();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
        
    }

    public ResultSet readTable(String tableName) throws Exception, SQLException{
    	
    	try {
    		
            this.resultSet = this.statement.executeQuery("select * from " + tableName);
            this.metaData = this.resultSet.getMetaData();
        } catch (SQLException e) {
        	switch (e.getErrorCode())
        	   {
        	      case 1086:
        	    	  System.err.println("SQLException caught: " + e.getErrorCode());
        	    	  System.err.println("File already exists under that name, please delete or rename "
        	    	  		+ "file and try again");        	    	 
        	    	  break;
        	      default:
        	    	  throw e;
        	   }        
        	}finally {
        	}
    	return this.resultSet;

    }
    
    
    public void writeToFile(ResultSet resultSet, String tempFilePath) throws SQLException, UnsupportedEncodingException, FileNotFoundException {
    	int num = resultSet.getMetaData().getColumnCount();
    	System.out.println("writing " + num + " columns to file");
    	PrintWriter writer = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(tempFilePath)), "UTF-8"));
    		    while (resultSet.next()) {
    		    	String row = "";
    	        	for(int i=1;i<=num;i++) {
    	        		row = row + "," + resultSet.getString(i);
   		        	}
    	        	row = row.replaceFirst(",", "");
    	        	row = row + "\n";
    	        	writer.write(row);
   		        }
    		   writer.close(); 
    }
    
    public ResultSet getResultSet() {
		return resultSet;
    }
    
    public ResultSetMetaData getMetaData() {
		return metaData;
    }
    
    
    public void close() throws Exception {
        try {
            if (resultSet != null) {
                resultSet.close();
            }

            if (statement != null) {
                statement.close();
            }

            if (connect != null) {
                connect.close();
            }   
        } catch (Exception e) {
        	throw (e);
        }
    }

    
    /*
    public String getFileLocation(String tableName, String URL, String uname, String pword) throws SQLException, ClassNotFoundException {
    	String username = "user=" + uname;
    	String password = "password=" + pword;
    	String credentials = username + "&" + password;
    	String connectString = URL + "?" + credentials;
    	
    	// This will load the MySQL driver, each DB has its own driver
        Class.forName("com.mysql.jdbc.Driver");
        // Setup the connection with the DB
        connect = DriverManager.getConnection(connectString);
        // create and execute statement
        statement = connect.createStatement();
        // find safe location to place file
        resultSet = statement.executeQuery("SELECT @@secure_file_priv");
        resultSet.next();
        // create file path string
        String fileLocation = resultSet.getString(1);
        String file = fileLocation.replace("\\", "/") + "outfile.csv";
    	return file;
    }
    */
}
