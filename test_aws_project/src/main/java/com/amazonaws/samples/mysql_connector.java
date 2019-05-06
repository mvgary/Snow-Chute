package com.amazonaws.samples;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

public class mysql_connector {
	protected Connection connect = null;
    private Statement statement = null;
    private ResultSet resultSet = null;
    private ResultSetMetaData metaData = null;
    private String username = null;
    private String password = null;
    private String url = null;
    private String tempFilePath = null;
	static Semaphore semaphore = new Semaphore(4);
	

    
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
    		System.out.println("executing query");
            this.resultSet = this.statement.executeQuery("select * from " + tableName + " limit 100;");
            if(this.resultSet.next()) {
            	System.out.println("Column 1, Row 1: " + this.resultSet.getString(1));
            }else {
            	System.out.println("Column 1, Row 1: NULL");
            }
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
    	        		row = row + "\t" + resultSet.getString(i);
   		        	}
    	        	row = row.replaceFirst("\t", "");
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

    
	public List<String> getTableNames(String db) throws SQLException {
    	List<String> tables = new ArrayList<>();
    	this.statement.executeQuery("use " + db +";");
    	ResultSet tableNames = this.statement.executeQuery("show tables;");
    	while(tableNames.next()) {
    		System.out.println(tableNames.getString(1));
        	tables.add(tableNames.getString(1));
    	}
    	return tables;
    }
	
	public List<String> getTableNames(String db, String schema) throws SQLException {
    	List<String> tables = new ArrayList<>();
    	this.statement.executeQuery("use " + db +";");
    	this.statement.executeQuery("use schema " + schema + ";");
    	ResultSet tableNames = this.statement.executeQuery("show tables;");
    	while(tableNames.next()) {
    		System.out.println(tableNames.getString(1));
        	tables.add(tableNames.getString(1));
    	}
    	return tables;
    }
    
    
	public List<String> getSchemaNames(String db) throws SQLException {
    	List<String> schemas = new ArrayList<>();
    	this.statement.executeQuery("use " + db +";");
    	ResultSet schemaNames = this.statement.executeQuery("show schemas;");
    	while(schemaNames.next()) {
    		System.out.println(schemaNames.getString(1));
        	schemas.add(schemaNames.getString(1));
    	}
    	return schemas;
    }
	
	

	public void useDB(String db) throws SQLException {
		this.statement.executeQuery("use " + db + ";");
	}

	
	public void useSchema(String schema) throws SQLException {
		this.statement.executeQuery("use schema " + schema + ";");
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
	
}
