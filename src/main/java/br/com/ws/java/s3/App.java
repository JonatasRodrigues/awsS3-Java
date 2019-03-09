package br.com.ws.java.s3;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * AWS S3 example
 *
 */
public class App 
{
	
	private static final String SUFFIX = "/";
	private static AmazonS3 s3client;
	
    public static void main( String[] args ){
    	String bucketName = "yourbucketname"; //Bucket name should not contain uppercase characters
    	String folderName = "yourfoldername";
    	String pathFile = "path/your/file";
    	
    	// credentials object identifying user for authentication
		// user must have AWSConnector and AmazonS3FullAccess
        AWSCredentials credentials = new BasicAWSCredentials("YourAccessKeyID", "YourSecretAccessKey");
       
        // create a client connection based on credentials
        createS3Client(credentials);
        //create bucket -- name must be unique for all S3 users
        createBucket(bucketName);
        //list all buckets
        listBuckets();
        //create a folder into a bucket
        createFolder(bucketName, folderName);
        //upload file
        uploadFile(bucketName, folderName,pathFile);
        //delete folder
        deleteFolder(bucketName, folderName);
        // delete bucket
        deleteBucket(bucketName);
        
    }
  
    private static void createS3Client(AWSCredentials credentials) {
       s3client = new AmazonS3Client(credentials);
    }
    
    private static void createBucket(String bucketName) {
        s3client.createBucket(bucketName);
    }
  
    private static void listBuckets() {
        for (Bucket bucket :  s3client.listBuckets()) {
        	System.out.println(" - " + bucket.getName());
        }
    }
    
    private static void createFolder(String bucketName, String folderName) {
    	// create meta-data for your folder and set content-length to 0
    	ObjectMetadata metadata = new ObjectMetadata();
    	metadata.setContentLength(0);
    	// create empty content
    	InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
    	// create a PutObjectRequest passing the folder name suffixed by /
    	PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
    				folderName + SUFFIX, emptyContent, metadata);
    	// send request to S3 to create folder
    	 s3client.putObject(putObjectRequest);
    }
    
    private static void uploadFile(String bucketName, String folderName, String pathFile) {
        String fileName = folderName + SUFFIX + "myImage.jpg";
        s3client.putObject(new PutObjectRequest(bucketName, fileName, 
        		new File(pathFile)) 
       //CannedAccessControlList.PublicRead sets file to public - By default, files are private in Amazon S3
      //  .withCannedAcl(CannedAccessControlList.PublicRead) 
        );
    }
    
    /**
	 * This method first deletes all the files in given folder and than the
	 * folder itself
	 */
	private static void deleteFolder(String bucketName, String folderName) {
		List<S3ObjectSummary> fileList = s3client.listObjects(bucketName, folderName).getObjectSummaries();
		for (S3ObjectSummary file : fileList) {
			s3client.deleteObject(bucketName, file.getKey());
		}
		s3client.deleteObject(bucketName, folderName);
	}
	
	private static void deleteBucket(String bucketName) {
		s3client.deleteBucket(bucketName);
	}
}
