import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;

public class utils {
	public static void deleteMsg (List<Message> messages,String myQueueUrl,AmazonSQS sqs){

		System.out.println("Deleting a message.\n");
		String messageReceiptHandle = messages.get(0).getReceiptHandle();
		sqs.deleteMessage(new DeleteMessageRequest()
				.withQueueUrl(myQueueUrl)
				.withReceiptHandle(messageReceiptHandle));
	}
	public static void upload_file(AmazonS3 S3 ,String filetouploadpath,String bucketname){

		File f = new File(filetouploadpath);
		PutObjectRequest por = new PutObjectRequest(bucketname, f.getName(),f);
		S3.putObject(por);
		System.out.println("The file "+filetouploadpath+" is uploaded");


	}
	public static void upload_file2(AmazonS3 S3 ,InputStream inputStream,String bucketname,String localAppName){

		ObjectMetadata metadata =new ObjectMetadata();
		PutObjectRequest por = new PutObjectRequest(bucketname,localAppName,inputStream,metadata);
		S3.putObject(por);
		System.out.println("The file is uploaded");


	}

	public static BufferedReader download_file(AmazonS3 S3,String bucketName,String  fileName){

		S3Object object = S3.getObject(new GetObjectRequest(bucketName, fileName));
		BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
		System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
		return reader;

	}
	public static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}


	public static void create_bucket(String bucketname,AmazonS3 S3){
		if (!S3.doesBucketExist(bucketname)) {
			S3.createBucket(bucketname);
			System.out.println("bucket created named: "+bucketname);
		}
	}

	public static String getECSuserData()  {
		String userData = "";

		List<String> line=null;
		try {
			line = Files.readAllLines(Paths.get("/users/studs/bsc/2016/omrila/workspace/Mevuzarot/bootm.sh"));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		for (int i=0;i<line.size();i++){
			userData+=line.get(i)+"\n";
		}

		String base64UserData = null;
		try {
			base64UserData = new String( Base64.encodeBase64( userData.getBytes( "UTF-8" )), "UTF-8" );
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return base64UserData;
	}




	public static String getECSuserDataWorker(AWSCredentials Credentials){
		StringBuilder builder = new StringBuilder();


		builder.append("#!/bin/sh\n");
		builder.append("BIN_DIR=/tmp \n");
		builder.append("AWS_ACCESS_KEY_ID=\""+Credentials.getAWSAccessKeyId()+"\"\n");
		builder.append("AWS_SECRET_ACCESS_KEY=\""+Credentials.getAWSSecretKey()+"\"\n");
		builder.append("cd $BIN_DIR\n");
		builder.append("mkdir -p $BIN_DIR/bootstrap_scripts\n");
		builder.append("export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY\n"); 
		builder.append("mkdir -p /home/ec2-user/.aws\n");
		builder.append("aws s3 cp s3://downloadlitalomri/worker.jar $BIN_DIR \n");
		builder.append("echo accessKey=$AWS_ACCESS_KEY_ID > $BIN_DIR/credi.prep\n");
		builder.append("echo secretKey=$AWS_SECRET_ACCESS_KEY >> $BIN_DIR/credi.prep\n");
		builder.append("yum -y install java-1.8.0\n");
		builder.append("yum -y remove java-1.7.0-openjdk\n");
		builder.append("java -jar $BIN_DIR/worker.jar \n");

		String base64UserData = null;
		try{
			base64UserData = new String( Base64.encodeBase64( builder.toString().getBytes( "UTF-8" )), "UTF-8" );
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return base64UserData;
		
	}

	public static void delete_file(String name,AmazonS3 S3,String buket) {

		ObjectListing lis=S3.listObjects(buket,name);
		List<S3ObjectSummary>lists=lis.getObjectSummaries();
		for (S3ObjectSummary list : lists) {
			S3.deleteObject(buket,list.getKey());	
		}
	}
}
 
