package com.amazonaws.samples;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.*;

public class jdbc_connector {
	private Connection connect = null;
    private Statement statement = null;
    //private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;

    public ResultSetMetaData readDataBase(String tableName, String URL, String uname, String pword, String localFilePath) throws Exception, SQLException{
    	String username = "user=" + uname;
    	String password = "password=" + pword;
    	String credentials = username; 
    			//+ "&" + password;
    	String connectString = URL + "?" + credentials;
    	ResultSetMetaData metadata = null;
        String filePath = "C:\\Users\\micha\\Downloads";
    	try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.jdbc.Driver");
            // Setup the connection with the DB
            connect = DriverManager.getConnection(connectString);
            // create and execute statement
            statement = connect.createStatement();
            // export data from mysql into local file
            //statement.executeQuery("select * into outfile '" + filePath + "' from " + tableName);
            // retrieve metadata
            resultSet = statement.executeQuery("select * from " + tableName);
            writeToFile(resultSet);
            metadata = resultSet.getMetaData();
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
            close();
        }
        return metadata;
    }


    // You need to close the resultSet
    private void close() {
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

        }
    }
    
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
    
    public void writeToFile(ResultSet resultSet) throws SQLException, UnsupportedEncodingException, FileNotFoundException {
    	File file = new File("C:\\Users\\micha\\Downloads");
    	int num = resultSet.getMetaData().getColumnCount();
    	System.out.println("writing " + num + " columns to file");
    	PrintWriter writer = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream("C:\\Users\\micha\\Downloads\\outfile.csv")), "UTF-8"));
    		    while (resultSet.next()) {
    		    	String row = "";
    	        	for(int i=1;i<=num;i++) {
    	        		row = row + "," + resultSet.getString(i);
   		        	}
    	        	row = row.replaceFirst(",", "");
    	        	row = row + "\n";
	        		System.out.println(row);
    	        	writer.write(row);
   		        }
    		   writer.close(); 
    		
    }

}
