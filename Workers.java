import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.EC2MetadataUtils;

public class Workers{
	public static String propertiesFilePath = "credi.prep";
	public static AWSCredentials Credentials;
	public static AmazonS3 S3;
	public static AmazonEC2 ec2;
	public static AmazonSQS sqs;
	public static String toManager;
	public static String fromManager;
	public static boolean terminate=false;
	public static boolean stop=false;
	public static int d;
	static public int[] statistics = new int[3];

	public static void main(String[] args){
		try {
			Credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
			ec2 = new AmazonEC2Client(Credentials);
			S3 = new AmazonS3Client(Credentials);
			sqs = new AmazonSQSClient(Credentials);
			System.out.println("Credentials + ec2 + s3 + sqs created");
		} catch (IOException e) {
			e.printStackTrace();
		}
		toManager= sqs.listQueues("ToManagerQueue").getQueueUrls().get(0);
		fromManager= sqs.listQueues("FromManagerQueue").getQueueUrls().get(0);

		while(!terminate){
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(fromManager);
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();

			for (Message message : messages) {

				Timer timer = new Timer();
				timer.schedule(new TimerTask() {
					public void run() {sqs.changeMessageVisibility(fromManager, message.getReceiptHandle(), 30000);}}, 25000);

				if (message.getBody().equals("Terminate")){	terminate=true; utils.deleteMsg(messages, fromManager, sqs);	} 
				if (message.getBody().equals("new dates to handle"))
				{
					String start_date=message.getMessageAttributes().get("StartDate").getStringValue();
					String end_date=message.getMessageAttributes().get("EndDate").getStringValue();


					String localAppName=message.getMessageAttributes().get("localAppName").getStringValue();

					double spTh=Double.parseDouble(message.getMessageAttributes().get("Speed-threshold").getStringValue());
					double diTh=Double.parseDouble(message.getMessageAttributes().get("Diameter-threshold").getStringValue());
					double miTh=Double.parseDouble(message.getMessageAttributes().get("Miss-threshold").getStringValue());
					File file=Danger_Analysis.outputWrite(start_date, end_date,spTh,diTh,miTh,localAppName,statistics);
					PutObjectRequest por = new PutObjectRequest("workermanagerliombucket", file.getName(),file);
					S3.putObject(por);
					System.out.println(file.getName());		


					Map<String,MessageAttributeValue> attributes = new HashMap<String,MessageAttributeValue>();
					attributes.put("localAppName", new MessageAttributeValue().withDataType("String").withStringValue(localAppName));
					sqs.sendMessage(new SendMessageRequest(toManager, "Job done").withMessageAttributes(attributes));
					System.out.println(statistics[0]);
					System.out.println(statistics[1]);
					System.out.println(statistics[2]);
					timer.cancel();
					utils.deleteMsg(messages, fromManager, sqs);
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
					
					}
				}//if_end new dates to handle
			}//end_message_for
		}//end_global_while
		/********making ststistics********/
		System.out.println("Sending statistics");
		File f = new File("Statistics"+UUID.randomUUID()+".txt");
		BufferedWriter bw;
		try {
			bw = new BufferedWriter(new FileWriter(f,true));
			bw.write("parsed astroid:"+statistics[0]+" Dengerous astroids:"+statistics[1]+" totally safe:"+statistics[2]);
			bw.close();
			PutObjectRequest por = new PutObjectRequest("workersstatisticsbucet", f.getName(),f);
			S3.putObject(por);
		} catch (IOException e) {
			e.printStackTrace();
		}

		/*********sent shut down instance*********/
		System.out.println("send the manager a terminate request couse im ready to end my life");
		String myPersonalId=EC2MetadataUtils.getInstanceId();
		Map<String,MessageAttributeValue> attributes = new HashMap<String,MessageAttributeValue>();
		attributes.put("Id", new MessageAttributeValue().withDataType("String").withStringValue(myPersonalId));
		sqs.sendMessage(new SendMessageRequest(toManager, "Kill me").withMessageAttributes(attributes));
	}
}