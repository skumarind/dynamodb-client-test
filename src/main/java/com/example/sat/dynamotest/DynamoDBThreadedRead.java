package com.example.sat.dynamotest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.example.sat.dynamotest.model.TestObject;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by Satish on 1/2/2019.
 */
public class DynamoDBThreadedRead {

    static AmazonDynamoDB dynamoDBClient;
    static String TABLE_NAME = "my-test-table";
    static List<Integer> idList = new ArrayList<>();

    public static void main(String[] args) throws InterruptedException {
        long currentTimeStamp = System.currentTimeMillis();
        System.out.print("Start time:");
        System.out.println(new Date());
        dynamoDBClient = DynamoSetup.init();
        getHundredRecords();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

        idList.forEach(item -> {
            executor.submit(() -> {
                long threadStartTimeStamp = System.currentTimeMillis();
                if((threadStartTimeStamp - currentTimeStamp) <= (13*60*1000)) {
                    System.out.println(Thread.currentThread().getId() + "----Name :" + Thread.currentThread().getName());
                    System.out.println("Processing item:" + item);
                    try {
                        Thread.sleep(20000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        System.out.println("Dynamo process thread interrupted");
                    }
                    updateItem(item);
                }else{
                    System.out.println("Time limit exceeded");
                }
            });
        });

        awaitTerminationAfterShutdown(executor);

        int queueSize = 0;
        do{
            Thread.sleep(1000);
            queueSize = executor.getQueue().size();
            System.out.println("Current queue depth:"+queueSize);
        }while (queueSize != 0);
        System.out.print("End time:");
        System.out.println(new Date());
    }

    public static void awaitTerminationAfterShutdown(ExecutorService threadPool) {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(10, TimeUnit.MINUTES)) {
                threadPool.shutdownNow();
            }else{
                System.out.println("Task completed !!");
            }
        } catch (InterruptedException ex) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static void updateItem(int id) {
        DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);

        Table table = dynamoDB.getTable(TABLE_NAME);
        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("empId", id)
                .withUpdateExpression("set empStatus = :s")
                .withValueMap(new ValueMap().withString(":s", "ALPHA"))
                .withReturnValues(ReturnValue.UPDATED_NEW);
        try {
            System.out.println("Updating the item...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

        }
        catch (Exception e) {
            System.err.println("Unable to update item: " + id);
            System.err.println(e.getMessage());
        }
    }

    private static void getHundredRecords() {
        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDBClient);
        //DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
        //Table table = dynamoDB.getTable("my-test-table");
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
        expressionAttributeValues.put(":val", new AttributeValue().withN("101"));
        DynamoDBQueryExpression<TestObject> queryExpression = new DynamoDBQueryExpression<TestObject>()
                .withKeyConditionExpression("empId < :val1").withExpressionAttributeValues(expressionAttributeValues);


        ScanRequest scanRequest = new ScanRequest()
                .withTableName(TABLE_NAME)
                .withFilterExpression("empId < :val")
                .withExpressionAttributeValues(expressionAttributeValues);

        Map<String,AttributeValue> lastKey = null;
        do {

            ScanResult scanResult = dynamoDBClient.scan(scanRequest);

            List<Map<String,AttributeValue>> results = scanResult.getItems();
            results.forEach(r->idList.add(Integer.parseInt(r.get("empId").getN())));
            lastKey = scanResult.getLastEvaluatedKey();
            scanRequest.setExclusiveStartKey(lastKey);
        } while (lastKey!=null);
        Iterator<Integer> iterator = idList.iterator();
        System.out.println("100 names");
        while(iterator.hasNext()){
            System.out.println(iterator.next());
        }

    }
}
