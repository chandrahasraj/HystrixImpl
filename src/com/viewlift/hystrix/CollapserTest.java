package com.viewlift.hystrix;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixEventType;
import com.netflix.hystrix.HystrixRequestLog;
import com.netflix.hystrix.strategy.concurrency.HystrixRequestContext;

public class CollapserTest {
	
	private final static String tableName = "my-favorite-movies-table";
	private static AmazonDynamoDBClient dynamoDB;
	
	
	public static AmazonDynamoDBClient getDynamoDB() {
		return dynamoDB;
	}

	public static void setDynamoDB(AmazonDynamoDBClient dynamoDB) {
		CollapserTest.dynamoDB = dynamoDB;
	}

	public static void main(String as[]) throws Exception{
//		DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.defaultClient());

		init();
		TableUtils.waitUntilActive(dynamoDB, tableName);
		new CollapserTest().testDynamoCollapser();
	}
	
	public void testDynamoCollapser() throws InterruptedException, ExecutionException{
		HystrixRequestContext context = HystrixRequestContext.initializeContext();
		try{
	    Future<PutItemResult> f1 = new DynamoDBCollapser(newItem("name1",2017,"5","fan1"),tableName).queue();
        Future<PutItemResult> f2 = new DynamoDBCollapser(newItem("name2",2017,"4","fan2"),tableName).queue();
        Future<PutItemResult> f3 = new DynamoDBCollapser(newItem("name3",2017,"4.5","fan3"),tableName).queue();
        Future<PutItemResult> f4 = new DynamoDBCollapser(newItem("name4",2017,"3","fan4"),tableName).queue();
        
        System.out.println("F1 response-->"+f1.get());
        System.out.println("F2 response-->"+f2.get());
        System.out.println("F3 response-->"+f3.get());
        System.out.println("F4 response-->"+f4.get());
        
     // assert that the batch command 'GetValueForKey' was in fact
        // executed and that it executed only once
        System.out.println(HystrixRequestLog.getCurrentRequest().getExecutedCommands().size());
        HystrixCommand<?> command = HystrixRequestLog.getCurrentRequest().getExecutedCommands().toArray(new HystrixCommand<?>[1])[0];
        // assert the command is the one we're expecting
        System.out.println("GetValueForKey-->"+command.getCommandKey().name());
        // confirm that it was a COLLAPSED command execution
        System.out.println(command.getExecutionEvents().contains(HystrixEventType.COLLAPSED));
        // and that it was successful
        System.out.println(command.getExecutionEvents().contains(HystrixEventType.SUCCESS));
		}finally{
			context.shutdown();
		}
	}

	 private static Map<String, AttributeValue> newItem(String name, int year, String rating, String... fans) {
	        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
	        item.put("name", new AttributeValue(name));
	        item.put("year", new AttributeValue().withN(Integer.toString(year)));
	        item.put("rating", new AttributeValue(rating));
	        item.put("fans", new AttributeValue().withSS(fans));

	        return item;
	    }
	 
	 public static void init() throws Exception {
			/*
			 * The ProfileCredentialsProvider will return your [default] credential
			 * profile by reading from the credentials file located at
			 * (C:\\Users\\viewlift\\.aws\\credentials).
			 */
			AWSCredentials credentials = null;
			try {
				credentials = new ProfileCredentialsProvider("default")
						.getCredentials();
			} catch (Exception e) {
				throw new AmazonClientException(
						"Cannot load the credentials from the credential profiles file. "
								+ "Please make sure that your credentials file is at the correct "
								+ "location (C:\\Users\\viewlift\\.aws\\credentials), and is in valid format.",
						e);
			}
			dynamoDB = new AmazonDynamoDBClient(credentials);
			Region usWest2 = Region.getRegion(Regions.US_WEST_2);
			dynamoDB.setRegion(usWest2);
		}
}
