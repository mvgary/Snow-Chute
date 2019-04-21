package com.amazonaws.samples;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JTextArea;
import java.awt.FlowLayout;
import javax.swing.JFrame;
import javax.swing.JTextField;
import javax.swing.*; 
import de.javasoft.plaf.synthetica.SyntheticaClassyLookAndFeel;

public class GUI{
	static JTextField sqlURL, sqltableName, sqlusername, sqlpassword, clientRegion, bucketName, keyName, user,
	password, account, db, table, schema, SnowflakeConnectString, s3Path, aws_key_id, aws_secret_key, message_box;

  public static void main(String[] args) throws Exception {

	
	  
	  JFrame f = new JFrame("Text Field Examples");
		UIManager.setLookAndFeel(new SyntheticaClassyLookAndFeel());
	  f.setPreferredSize(new Dimension(300, 340));
	    f.setLocation(550,200);
	  f.getContentPane().setLayout(new FlowLayout());
	sqlURL = new JTextField("jdbc:mysql://ensembldb.ensembl.org:3306/zonotrichia_albicollis_rnaseq_96_101",10);
	sqltableName = new JTextField("analysis",10);
	sqlusername = new JTextField("anonymous",10);
	sqlpassword = new JTextField("bigmike",10);
	clientRegion = new JTextField("us-west-1",10);
	bucketName = new JTextField("mvgary-test-bucket-123494",10);
	keyName = new JTextField("test_key1",10);
	user = new JTextField("mvgary094",10);
	password = new JTextField("Bigmike094",10);
	account = new JTextField("ot24432",10);
	db = new JTextField("salesDB",10);
	table = new JTextField("analysis",10);
	schema = new JTextField("salesSchema",10);
	SnowflakeConnectString = new JTextField("jdbc:snowflake://ot24432.snowflakecomputing.com/",10);
	s3Path = new JTextField("s3://mvgary-test-bucket-123494/test_key1",10);
	aws_key_id = new JTextField("AKIAIBXWU6SVPE6TRESQ",10);
	aws_secret_key = new JTextField("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",10);
    f.getContentPane().add(BorderLayout.CENTER, sqlURL);
	f.getContentPane().add(BorderLayout.CENTER, sqltableName);
	f.getContentPane().add(BorderLayout.CENTER, sqlusername);
	f.getContentPane().add(BorderLayout.CENTER, sqlpassword);
	f.getContentPane().add(BorderLayout.CENTER, clientRegion);
	f.getContentPane().add(BorderLayout.CENTER, bucketName);
	f.getContentPane().add(BorderLayout.CENTER, keyName);
	f.getContentPane().add(BorderLayout.CENTER, user);
	f.getContentPane().add(BorderLayout.CENTER, password);
	f.getContentPane().add(BorderLayout.CENTER, account);
	f.getContentPane().add(BorderLayout.CENTER, db);
	f.getContentPane().add(BorderLayout.CENTER, table);
	f.getContentPane().add(BorderLayout.CENTER, schema);
	f.getContentPane().add(BorderLayout.CENTER, SnowflakeConnectString);
	f.getContentPane().add(BorderLayout.CENTER, s3Path);
	f.getContentPane().add(BorderLayout.CENTER, aws_key_id);
	f.getContentPane().add(BorderLayout.WEST, aws_secret_key);
	
    final JButton button = new JButton("Begin Import");
    button.setPreferredSize(new Dimension(123,28));
    f.getContentPane().add(BorderLayout.SOUTH, button);
    
	message_box = new JTextField(22);
    f.getContentPane().add(BorderLayout.CENTER, message_box);

    f.pack();
	f.setVisible(true);
	//UIManager.setLookAndFeel(new SyntheticaClassyLookAndFeel());
    button.addActionListener(new ActionListener() {

        @Override
        public void actionPerformed(ActionEvent e) {
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
            //initialize Snowflake jdbc connector variable
            String user = "mvgary094";
            String password = "Bigmike094";
            String account = "ot24432";
            String db = "salesDB";
            String table = "product4";
            String schema = "salesSchema";
            String SnowflakeConnectString = "jdbc:snowflake://ot24432.snowflakecomputing.com/";
            String s3Path = "s3://mvgary-test-bucket-123494/test_key1";
            String aws_key_id = "AKIAIBXWU6SVPE6TRESQ";
            String aws_secret_key = "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";
        	*/
            test_class ts = new test_class();
            try {
                String sqlURLString = sqlURL.getText();
                String sqltableNameString = sqltableName.getText();
                String sqlusernameString = sqlusername.getText();
                String sqlpasswordString = sqlpassword.getText();
                String clientRegionString = clientRegion.getText();
                String bucketNameString = bucketName.getText();
                String keyNameString = keyName.getText();
                String userString = user.getText();
                String passwordString = password.getText();
                String accountString = account.getText();
                String dbString = db.getText();
                String tableString = table.getText();
                String schemaString = schema.getText();
                String SnowflakeConnectStringString = SnowflakeConnectString.getText();
                String s3PathString = s3Path.getText();
                String aws_key_idString = aws_key_id.getText();
                String aws_secret_keyString = aws_secret_key.getText();
				ts.importTable(sqlURLString, sqltableNameString, sqlusernameString, sqlpasswordString,
						clientRegionString, bucketNameString, keyNameString, userString, passwordString,
						accountString, dbString, tableString, schemaString, SnowflakeConnectStringString,
						s3PathString, aws_key_idString, aws_secret_keyString);
				String message = "table " + tableString + " imported";
				message_box.setText(message);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

        }
    });

    f.setVisible(true);

  }

}