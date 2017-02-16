package com.viewlift.hystrix;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.netflix.hystrix.HystrixCollapser;
import com.netflix.hystrix.HystrixCommand;

public class DynamoDBCollapser extends HystrixCollapser<List<PutItemResult>, PutItemResult,Map<String,AttributeValue>>{

	private final Map<String, AttributeValue> newItem;
	private final String tableName;
	
	public DynamoDBCollapser(Map<String, AttributeValue> newItem,String tableName) {
		this.newItem = newItem;
		this.tableName = tableName;
	}

	@Override
	public Map<String, AttributeValue> getRequestArgument() {
		return newItem;
	}

	@Override
	protected HystrixCommand<List<PutItemResult>> createCommand(
			Collection<com.netflix.hystrix.HystrixCollapser.CollapsedRequest<PutItemResult, Map<String, AttributeValue>>> requests) {
		Iterator<com.netflix.hystrix.HystrixCollapser.CollapsedRequest<PutItemResult, Map<String, AttributeValue>>> cr = requests.iterator();
		List<PutItemRequest> list = new ArrayList<>();
		while(cr.hasNext()){
			list.add(new PutItemRequest(tableName,cr.next().getArgument()));
		}
		return new DynamoDBBatchCommand	(list);
	}

	@Override
	protected void mapResponseToRequests( List<PutItemResult> batchResponse, Collection<com.netflix.hystrix.HystrixCollapser.CollapsedRequest<PutItemResult, Map<String, AttributeValue>>> requests) {
		int count  = 0;
		for(CollapsedRequest<PutItemResult,Map<String,AttributeValue>> request:requests){
			request.setResponse(batchResponse.get(count++));
		}
	}

}
