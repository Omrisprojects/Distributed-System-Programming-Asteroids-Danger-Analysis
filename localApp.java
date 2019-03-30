import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;


public class localApp {

	public static AWSCredentials Credentials;
	public static AmazonS3 S3;
	public static AmazonEC2 ec2;
	public static AmazonSQS sqs;

	public static String propertiesFilePath = "/users/studs/bsc/2016/omrila/workspace/Mevuzarot/omri.properties";
	public static String fileToUploadPath;
	public static String bucketName = "litalsandomrisbucket";
	public static String QueueName="managerlocalqueue";
	public static String localAppName="LocalApp"+ UUID.randomUUID();
	public static String inPutName;
	public static String outputName;
	public static String sqsUrl;
	public static String manager_id;
	public static String managerSsqsUrl;
	public static String fromManagerSsqsUrl;
	public static String d;
	public static String n;
	
	public static void main(String[] args) throws Exception {

		inPutName=args[0];
		outputName=args[1];
		n=args[2];
		d=args[3];
		boolean end=false;

		Credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
		ec2 = new AmazonEC2Client(Credentials);
		S3 = new AmazonS3Client(Credentials);
		sqs = new AmazonSQSClient(Credentials);
		System.out.println("Credentials + ec2 + s3 + sqs created");

		manager_id=create_manager();
		utils.create_bucket(bucketName,S3);
		utils.create_bucket("workermanagerliombucket",S3);
		utils.create_bucket("workersstatisticsbucet",S3);

		fileToUploadPath = "/users/studs/bsc/2016/omrila/workspace/Mevuzarot/"+inPutName;
		utils.upload_file(S3,fileToUploadPath,bucketName);


		/**************************sqs*********************************/

		CreateQueueRequest createQueueRequest = new CreateQueueRequest(localAppName);
		sqsUrl = sqs.createQueue(createQueueRequest).getQueueUrl(); // create a Queue
		System.out.println("local up name:"+localAppName);
		/************************loclAppFinish?************************************/

		createQueueRequest = new CreateQueueRequest("ToManagerQueue");
		managerSsqsUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
		createQueueRequest = new CreateQueueRequest("FromManagerQueue");
		fromManagerSsqsUrl=sqs.createQueue(createQueueRequest).getQueueUrl();

		Map<String,MessageAttributeValue> attributes = new HashMap<String,MessageAttributeValue>();

		attributes.put("BucketName", new MessageAttributeValue().withDataType("String").withStringValue(bucketName));
		attributes.put("FileName", new MessageAttributeValue().withDataType("String").withStringValue(inPutName));
		attributes.put("n", new MessageAttributeValue().withDataType("String").withStringValue(n));
		attributes.put("d", new MessageAttributeValue().withDataType("String").withStringValue(d));
		attributes.put("localAppName", new MessageAttributeValue().withDataType("String").withStringValue(localAppName));

		sqs.sendMessage(new SendMessageRequest(managerSsqsUrl, "new task to prosses").withMessageAttributes(attributes));
		System.out.println("New task message is sent to the manager");


		while (!end){
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqs.getQueueUrl(localAppName).getQueueUrl());
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			if(messages.size()==0){
				Thread.sleep(10000);
			}
			else for (Message message : messages) {
				if (message.getBody().equals("check for final file")){

					BufferedReader reader=utils.download_file(S3, "litalsandomrisbucket", localAppName);
					String line = " ";
					try {
						line = utils.readAll(reader);
					} catch (IOException e) {
						e.printStackTrace();
					}
					File finaly=new File(outputName+".html");
					BufferedWriter bw = new BufferedWriter(new FileWriter(finaly,true));
					bw.write(line);
					bw.close();
					end=true;
					System.out.println("The summary file is ready");
					
					utils.deleteMsg(messages, sqsUrl, sqs);
					utils.delete_file(localAppName, S3,"litalsandomrisbucket");
					utils.delete_file(args[0], S3, "litalsandomrisbucket");
					
				}
			}
		}
		if (args.length>4){
			end=false;
			Map<String,MessageAttributeValue> attribute = new HashMap<String,MessageAttributeValue>();
			attribute.put("localAppName", new MessageAttributeValue().withDataType("String").withStringValue(localAppName));
			sqs.sendMessage(new SendMessageRequest(managerSsqsUrl, "Terminate").withMessageAttributes(attribute));
			System.out.println("Sent the mannager a terminate message");
			while (!end){
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqs.getQueueUrl(localAppName).getQueueUrl());
				List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				if(messages.size()==0){
					Thread.sleep(10000);
				}
				else for (Message message : messages) {
					System.out.println("I got the manager terminate msg");
					if (message.getBody().equals("terminate")&&(args.length>4)){
						terminate(); 
						end=true;
					}
				}
			}
		}
		
		try{
			DeleteQueueRequest  deleteQueueRequest = new DeleteQueueRequest(sqsUrl);
			sqs.deleteQueue(deleteQueueRequest);
			if (args.length>4){
				DeleteQueueRequest  deleteQueueRequest2 = new DeleteQueueRequest(managerSsqsUrl);
				sqs.deleteQueue(deleteQueueRequest2);
				DeleteQueueRequest  deleteQueueRequest3 = new DeleteQueueRequest(fromManagerSsqsUrl);
				sqs.deleteQueue(deleteQueueRequest3);
			}
		}catch(QueueDoesNotExistException e){
			System.out.println("queue already deleted");
		}
		
		
		
		
	}

	//Terminate Instances
	public static void terminate(){
		List<String> instancesToTerminate = new ArrayList<String>();
		instancesToTerminate.add(manager_id);
		TerminateInstancesRequest terminate_request = new TerminateInstancesRequest();
		terminate_request.setInstanceIds(instancesToTerminate);
		ec2.terminateInstances(terminate_request);
		System.out.println("the manger is dead");
	}


	public static String create_manager(){
		boolean is_Manager_Alive = false;
		/********************************manager alive?**********************************************/
		DescribeInstancesRequest request = new DescribeInstancesRequest();

		List<String> valuesJob = new ArrayList<String>();
		valuesJob.add("Manager");
		Filter filter1 = new Filter("tag:Job", valuesJob);

		List<String> statusValues = new ArrayList<String>();
		statusValues.add("pending");
		statusValues.add("running");

		Filter statusTag = new Filter("instance-state-name", statusValues);

		DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter1,statusTag));
		List<Reservation> reservations = result.getReservations();
		if(reservations.size()>0){
			for (Reservation reservation : reservations) {
				List<Instance> instances = reservation.getInstances();

				System.out.println("manager already alive");
				is_Manager_Alive=true;
				for (Instance instance : instances) {
					System.out.println(instance.getInstanceId());
					return instance.getInstanceId();
				}
			}
		}


		/**************************create manager if he is not alive*********************************/
		if (!is_Manager_Alive){
			System.out.println("Creating Manager");
			RunInstancesRequest request2 = new RunInstancesRequest("ami-b66ed3de", 1, 1).withUserData(utils.getECSuserData());
			request2.setInstanceType(InstanceType.T2Micro.toString());
			List<Instance> instances = ec2.runInstances(request2).getReservation().getInstances();
			System.out.println("Manager created");
			CreateTagsRequest ctr = new CreateTagsRequest().withResources(instances.get(0).getInstanceId()).withTags(new Tag("Job","Manager"));
			ec2.createTags(ctr);
			return instances.get(0).getInstanceId();
		}
		return manager_id;
	} 
}


