import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONObject;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class Manager {

	public static String bucketName = "workermanagerliombucket";
	public static String propertiesFilePath = "credi.prep";
	public static AWSCredentials Credentials;
	public static AmazonS3 S3;
	public static AmazonEC2 ec2;
	public static AmazonSQS sqs;
	public static boolean terminate=false;
	public static String toManager;
	public static String fromManager;

	public static void main(String[] args) {

		Map<String,Integer> tasks = new HashMap<String,Integer>();
		Map<String,Integer> maxWorkersNeeded = new HashMap<String,Integer>();

		try {
			Credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
		} catch (IOException e) {
			e.printStackTrace();
		}
		ec2 = new AmazonEC2Client(Credentials);
		S3 = new AmazonS3Client(Credentials);
		sqs = new AmazonSQSClient(Credentials);
		System.out.println("Credentials + ec2 + s3 + sqs created");

		toManager= sqs.listQueues("ToManagerQueue").getQueueUrls().get(0);
		fromManager= sqs.listQueues("FromManagerQueue").getQueueUrls().get(0);
		boolean no_more_jobs=false;
		String terlocalAppName=null;
		int max;

		while(!terminate){

			max=0;
			for (int map:maxWorkersNeeded.values())
			{
				if(map>max){
					max=map;
				}
			}
			if (max>workersRunning()){
				WorkersToOpen(max);		
			}

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(toManager);
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();

			for (Message message : messages) {
				/**************************************new task to prosses*******************************/
				if (message.getBody().equals("new task to prosses"))
				{
					if (no_more_jobs){
						System.out.println("i dont want to receive any more messages");
						utils.deleteMsg(messages, toManager, sqs);
					}else{
						System.out.println("manager is working on a new task");
						System.out.println(message.getMessageAttributes().get("BucketName").getStringValue());
						System.out.println(message.getMessageAttributes().get("FileName").getStringValue());
						String localAppName=message.getMessageAttributes().get("localAppName").getStringValue();

						BufferedReader reader=utils.download_file(S3, "litalsandomrisbucket", message.getMessageAttributes().get("FileName").getStringValue());
						String line = "";
						try {
							line = utils.readAll(reader);
						} catch (IOException e) {
							e.printStackTrace();
						}
						JSONObject jsonObject=Danger_Analysis.inputRead(line);
						String start_date=(String)jsonObject.get("start_date");
						String end_date=(String)jsonObject.get("end_date");
						String speedthreshold=(String)jsonObject.get("speed-threshold");
						String diameterthreshold=(String)jsonObject.get("diameter-threshold");
						String missthreshold=(String)jsonObject.get("miss-threshold");

						int n=Integer.parseInt(message.getMessageAttributes().get("n").getStringValue());
						int d=Integer.parseInt(message.getMessageAttributes().get("d").getStringValue());

						String curr_start=start_date;
						String curr_end=start_date;
						String bucketName=message.getMessageAttributes().get("BucketName").getStringValue();
						int numWorkersOpen=0;
						while(!((curr_start).equals(date.Get_Next_Date(end_date)))){
							try {
								curr_end=(date.daysHandler(curr_start, end_date, d-1));
							} catch (ParseException e) {
								e.printStackTrace();
							}

							Map<String,MessageAttributeValue> attributes = new HashMap<String,MessageAttributeValue>();

							attributes.put("BucketName", new MessageAttributeValue().withDataType("String").withStringValue(bucketName));
							attributes.put("StartDate", new MessageAttributeValue().withDataType("String").withStringValue(curr_start));
							attributes.put("EndDate", new MessageAttributeValue().withDataType("String").withStringValue(curr_end));
							attributes.put("Speed-threshold", new MessageAttributeValue().withDataType("String").withStringValue(speedthreshold));
							attributes.put("Diameter-threshold", new MessageAttributeValue().withDataType("String").withStringValue(diameterthreshold));
							attributes.put("Miss-threshold", new MessageAttributeValue().withDataType("String").withStringValue(missthreshold));
							attributes.put("d", new MessageAttributeValue().withDataType("String").withStringValue(Integer.toString(d)));
							attributes.put("fromManagerQueue", new MessageAttributeValue().withDataType("String").withStringValue(fromManager));
							attributes.put("localAppName", new MessageAttributeValue().withDataType("String").withStringValue(localAppName));

							sqs.sendMessage(new SendMessageRequest(fromManager, "new dates to handle").withMessageAttributes(attributes));


							numWorkersOpen++;
							System.out.println("send new dates to handle");
							curr_start=date.Get_Next_Date(curr_end);

						}
						tasks.put(localAppName,numWorkersOpen);


						double numbersToOpen=numWorkersOpen;
						double doubleN=n;
						numbersToOpen=Math.ceil(numbersToOpen/doubleN);
						numWorkersOpen=(int)numbersToOpen;

						utils.deleteMsg(messages, toManager, sqs);
						WorkersToOpen(numWorkersOpen);
						maxWorkersNeeded.put(localAppName,numWorkersOpen);
					}
				}
				/**************************************Job done to prosses*******************************/

				if (message.getBody().equals("Job done"))
				{

					String bucketName="litalsandomrisbucket";

					String localAppName=message.getMessageAttributes().get("localAppName").getStringValue();

					int i=tasks.get(localAppName).intValue();
					i--;
					tasks.put(localAppName,i);
					System.out.println(i);
					utils.deleteMsg(messages, toManager, sqs);
					if(i==0){
						String input = "";
						ObjectListing lis=S3.listObjects("workermanagerliombucket","days_"+localAppName);
						List<S3ObjectSummary>lists=lis.getObjectSummaries();
						for (S3ObjectSummary list : lists) {
							String name=list.getKey();
							System.out.println(name);
							BufferedReader summ=utils.download_file(S3, "workermanagerliombucket", name);
							String line = "";
							try {
								line = utils.readAll(summ);
							} catch (IOException e) {
								e.printStackTrace();
							}
							input=input+"\n"+line;
							S3.deleteObject("workermanagerliombucket",list.getKey());	

						}

						InputStream is = new ByteArrayInputStream(input.getBytes());
						utils.upload_file2(S3,is,bucketName,localAppName);
						sqs.sendMessage(new SendMessageRequest(localAppName, "check for final file"));
						tasks.remove(localAppName);
						maxWorkersNeeded.remove(localAppName);

						max=0;
						for (int map:maxWorkersNeeded.values())
						{
							if(map>max){
								max=map;
							}
						}
						if (max<workersRunning()){
							for (int j=0; j<(workersRunning()-max);j++)
							{sqs.sendMessage(new SendMessageRequest(fromManager, "Terminate"));}
						}						
					}
				}
				/**************************************Terminate from localApp prosses*******************************/
				if (message.getBody().equals("Terminate")){
					System.out.println("Terminate message receive");
					no_more_jobs=true;
					terlocalAppName=message.getMessageAttributes().get("localAppName").getStringValue();
					utils.deleteMsg(messages, toManager, sqs);
					System.out.println("Terminate msg end");
				}
				/**************************************Terminate from worker prosses*******************************/

				if (message.getBody().equals("Kill me")){
					System.out.println("worker requset to die");
					terminateWorker(message.getMessageAttributes().get("Id").getStringValue());
					utils.deleteMsg(messages, toManager, sqs);
				}
			}
			if (no_more_jobs){
				System.out.println("no_more_jobs printing");
				if(tasks.size()==0){
					int count=workersRunning();
					for (int i=0; i<count;i++){
						sqs.sendMessage(new SendMessageRequest(fromManager, "Terminate"));
					}
					boolean allWorkersAreDead=false;
					while (!allWorkersAreDead){
						System.out.println("all workers are dead waiting");
						if (workersRunning()==0){
							System.out.println("sent a terminate msg to the LocalApp");
							allWorkersAreDead=true;
							sqs.sendMessage(new SendMessageRequest(terlocalAppName, "terminate"));
							terminate=true;	
						}
						ReceiveMessageRequest receiveMessageRequest2 = new ReceiveMessageRequest(toManager);
						List<Message> messages2 = sqs.receiveMessage(receiveMessageRequest2.withMessageAttributeNames("All")).getMessages();

						for (Message message2 : messages2) {
							if (message2.getBody().equals("Kill me")){
								terminateWorker(message2.getMessageAttributes().get("Id").getStringValue());
								utils.deleteMsg(messages2, toManager, sqs);
							}
						}
					}
				}
			}
		}

	}
	/**************************************Terminate worker*******************************/

	public static void terminateWorker(String workerId){
		List<String> instancesToTerminate = new ArrayList<String>();
		instancesToTerminate.add(workerId);
		TerminateInstancesRequest terminate_request = new TerminateInstancesRequest();
		terminate_request.setInstanceIds(instancesToTerminate);
		ec2.terminateInstances(terminate_request);
		System.out.println("worker is dead");
	}

	public static void WorkersToOpen(int numWorkersOpen){
		int running=workersRunning();
		numWorkersOpen-=running;
		System.out.println("currently running:"+running);
		System.out.println("need to be open:  "+numWorkersOpen);

		if(numWorkersOpen>0){

			RunInstancesRequest request = new RunInstancesRequest("ami-b66ed3de", numWorkersOpen,numWorkersOpen).withUserData(utils.getECSuserDataWorker(Credentials));
			request.setInstanceType(InstanceType.T2Micro.toString());
			List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
			System.out.println("workers created");
			for(int i=0;i<instances.size();i++){
				CreateTagsRequest ctr = new CreateTagsRequest().withResources(instances.get(i).getInstanceId()).withTags(new Tag("work","worker"));
				ec2.createTags(ctr);
			}
		}


	}
	/**************************************check how much workers running*******************************/

	public static int workersRunning(){
		DescribeInstancesRequest request = new DescribeInstancesRequest();

		List<String> valuesJob = new ArrayList<String>();
		valuesJob.add("worker");
		Filter filter1 = new Filter("tag:work", valuesJob);

		List<String> statusValues = new ArrayList<String>();
		statusValues.add("running");
		statusValues.add("pending");

		Filter statusTag = new Filter("instance-state-name", statusValues);


		DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter1,statusTag));
		List<Reservation> reservations = result.getReservations();
		int aliveWorkers = 0;
		for(Reservation res: reservations){
			aliveWorkers += res.getInstances().size();
		}

		return aliveWorkers;
	}
}



