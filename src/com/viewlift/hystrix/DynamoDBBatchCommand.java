package com.viewlift.hystrix;

import static com.netflix.hystrix.HystrixCommandGroupKey.Factory.asKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.netflix.hystrix.HystrixCommand;

public class DynamoDBBatchCommand extends HystrixCommand<List<PutItemResult>> {

	private final List<PutItemRequest> list;
	

	protected DynamoDBBatchCommand(List<PutItemRequest> list) {
		super(Setter.withGroupKey(asKey("db batch")));
		this.list = list;
	}

	@Override
	protected List<PutItemResult> run() throws Exception {
		List<PutItemResult> result = new ArrayList<>();
		BatchWriteItemRequest batchRequest = new BatchWriteItemRequest();
		List<WriteRequest> wrs = new ArrayList<>();
		Map<String, List<WriteRequest>> map = new HashMap<>();
		String tableName;
		if (list != null && !list.isEmpty())
			tableName = list.get(0).getTableName();
		else
			return null;

		for (PutItemRequest req : list) {
//			WriteRequest wr = new WriteRequest();
//			PutRequest pr = new PutRequest();
//			pr.setItem(req.getExpressionAttributeValues());
//			wr.setPutRequest(pr);
//			wrs.add(wr);
			result.add(CollapserTest.getDynamoDB().putItem(req));
		}
//		map.put(tableName, wrs);
//		batchRequest.setRequestItems(map);
//		CollapserTest.getDynamoDB().batchWriteItem(batchRequest);
		return result;
	}


}
