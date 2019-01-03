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

    private static AmazonDynamoDB dynamoDBClient;
    private static String TABLE_NAME = "my-test-table";
    private static List<Integer> idList = new ArrayList<>();
    private static List<Integer> workingIdList = new ArrayList<>();
    private static int workingIdListStart = 0;
    private static final long allowedTimeWindowForExecution = 5 * 60 * 1000;
    private static int numOfRecordsUpdated = 0;

    public static void main(String[] args) throws InterruptedException {
        long currentTimeStamp = System.currentTimeMillis();
        System.out.print("Start time:");
        System.out.println(new Date());
        dynamoDBClient = DynamoSetup.init();
        getALotOfRecords();
        do {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
            getHundredRecords();
            workingIdList.forEach(item -> {
                executor.submit(() -> {
                    long threadStartTimeStamp = System.currentTimeMillis();
                    if ((threadStartTimeStamp - currentTimeStamp) <= allowedTimeWindowForExecution) {
                        System.out.println(Thread.currentThread().getId() + "----Name :" + Thread.currentThread().getName());
                        System.out.println("Processing item:" + item);
                        updateItem(item);
                        try {
                            Thread.sleep(20000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            System.out.println("Dynamo process thread interrupted");
                        }
                        numOfRecordsUpdated++;
                    } else {
                        System.out.println("Time limit inside thread exceeded");
                    }
                });
            });
            awaitTerminationAfterShutdown(executor);
            workingIdList.clear();
        }while ((System.currentTimeMillis() - currentTimeStamp) <= allowedTimeWindowForExecution);

        System.out.println("Number of records updated :"+numOfRecordsUpdated); //  No correct count
        System.out.print("End time:");
        System.out.println(new Date());
    }

    private static void getHundredRecords() {
        System.out.println("Get Hundred records starting from :"+workingIdListStart);
        if(workingIdList.size() == 0){
            int i = 0;
            for( i = workingIdListStart ; i < 100+workingIdListStart ; i++){
                workingIdList.add(idList.get(i));
            }
            workingIdListStart = i;
        }
    }

    private static void awaitTerminationAfterShutdown(ExecutorService threadPool) {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(5, TimeUnit.MINUTES)) {
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

    private static void getALotOfRecords() {
        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDBClient);
        //DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
        //Table table = dynamoDB.getTable("my-test-table");
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
        expressionAttributeValues.put(":val", new AttributeValue().withN("10001"));
        DynamoDBQueryExpression<TestObject> queryExpression = new DynamoDBQueryExpression<TestObject>()
                .withKeyConditionExpression("empId < :val1").withExpressionAttributeValues(expressionAttributeValues);


        Map<String,AttributeValue> lastKey = null;
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(TABLE_NAME)
                .withLimit(100)
                .withFilterExpression("empId < :val")
                .withExpressionAttributeValues(expressionAttributeValues)
                .withExclusiveStartKey(lastKey);


        int scanCount = 1;
        do {
            System.out.println("Scan count:"+scanCount);

            ScanResult scanResult = dynamoDBClient.scan(scanRequest);

            List<Map<String,AttributeValue>> results = scanResult.getItems();
            results.forEach(r->idList.add(Integer.parseInt(r.get("empId").getN())));
            lastKey = scanResult.getLastEvaluatedKey();
            scanRequest.setExclusiveStartKey(lastKey);
            scanCount++;
        } while (lastKey!=null);
        Iterator<Integer> iterator = idList.iterator();
        System.out.println("100 names");
        while(iterator.hasNext()){
            System.out.println(iterator.next());
        }

    }
}
