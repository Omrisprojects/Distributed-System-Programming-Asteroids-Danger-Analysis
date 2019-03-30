import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class anothertest {


	public static String bucketName = "litalsandomrisbucket";
	public static AWSCredentials Credentials;
	public static AmazonS3 S3;
	public static AmazonEC2 ec2;
	public static AmazonSQS sqs;

	public static String propertiesFilePath = "/users/studs/bsc/2016/omrila/workspace/Mevuzarot/omri.properties";
	public static String fileToUploadPath = "/users/studs/bsc/2016/omrila/workspace/Mevuzarot/input.json";
	public static String inPutName;
	public static String outputName;
	public static String QueueName="managerlocalqueue";
	public static String localAppName="LocalApp"+ UUID.randomUUID();
	public static String sqsUrl;
	public static String d;
	public static String n;
	public static String manager_id;
	public static String managerSsqsUrl;


	public static void main(String[] args) throws Exception {


		Credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
		System.out.println("Credentials created.");
		ec2 = new AmazonEC2Client(Credentials);
		S3 = new AmazonS3Client(Credentials);
		sqs = new AmazonSQSClient(Credentials);
		
		
		String bucketName="litalsandomrisbucket";


		
			String input = "";
			ObjectListing lis=S3.listObjects("workermanagerliombucket");
			List<S3ObjectSummary>lists=lis.getObjectSummaries();
			for (S3ObjectSummary list : lists) {
	
					S3.deleteObject("workermanagerliombucket",list.getKey());	

			}
			System.out.println("bb");

		}
		
		
		
	}

