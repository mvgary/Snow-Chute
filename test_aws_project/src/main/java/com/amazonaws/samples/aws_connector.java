package com.amazonaws.samples;
import java.io.File;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

public class aws_connector {
	private AmazonS3 s3Client = null;
	private TransferManager tm = null;
	private String clientRegion = null;
	private String bucketName = null;
	
	public aws_connector(String region, String bucket) {
		this.clientRegion = region;
		this.bucketName = bucket;
		//build the s3 client and the transfer manager
        this.s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(clientRegion)
                .withCredentials(new ProfileCredentialsProvider())
                .build();
        this.tm = TransferManagerBuilder.standard()
                .withS3Client(s3Client)
                .build();
        
	}
	
	    public void S3Upload(String keyName, String filePath) throws Exception {
	    	
	        try {
	            // TransferManager processes all transfers asynchronously,
	            // so this call returns immediately.
	            Upload upload = tm.upload(bucketName, keyName, new File(filePath));
	            System.out.println("Object upload started");
	    
	            // Optionally, wait for the upload to finish before continuing.
	            upload.waitForCompletion();
	            System.out.println("Object upload complete");
	            File file = new File(filePath);
	            file.delete();
	            System.out.println("File deleted");
	        }
	        catch(AmazonServiceException e) {
	            // The call was transmitted successfully, but Amazon S3 couldn't process 
	            // it, so it returned an error response.
	            e.printStackTrace();
	        }
	        catch(SdkClientException e) {
	            // Amazon S3 couldn't be contacted for a response, or the client
	            // couldn't parse the response from Amazon S3.
	            e.printStackTrace();
	        }
	    }
	
}
