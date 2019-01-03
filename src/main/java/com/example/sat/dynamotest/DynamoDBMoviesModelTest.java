package com.example.sat.dynamotest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.example.sat.dynamotest.model.EmbeddedJson;
import com.example.sat.dynamotest.model.TestObject;
import com.example.sat.dynamotest.model.SomeJsonModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.RandomStringUtils;

public class DynamoDBMoviesModelTest {

	static AmazonDynamoDB dynamoDB;
	
	private static void init() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\Satish\\.aws\\credentials).
         */
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\Satish\\.aws\\credentials), and is in valid format.",
                    e);
        }
        dynamoDB = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion("eu-west-1")
            .build();
    }
	
	public static void main(String[] args) throws Exception  {
		init();
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();

		DynamoDBMapper mapper = new DynamoDBMapper(client);

		
		
		EmbeddedJson embeddedJson = new EmbeddedJson();
		embeddedJson.setEmbedOne("myEmbedValueOne");
		embeddedJson.setEmbedTwo("yourEmbedValueTwo");
		SomeJsonModel someJsonModel = new SomeJsonModel();
		someJsonModel.setEmbeddedJSON(embeddedJson);
		someJsonModel.setFirstName("Test First Name");
		someJsonModel.setLastName("Test Last Name");
		ObjectMapper objectMapper = new ObjectMapper();
		String someJsonObj = objectMapper.writeValueAsString(someJsonModel);

		for(int i = 0 ; i < 999 ; i++) {

			int length = 10;
			boolean useLetters = true;
			boolean useNumbers = false;
			String generatedString = RandomStringUtils.random(length, useLetters, useNumbers);

			System.out.println(generatedString);
			TestObject item = new TestObject();
			item.setId(i+1);
			item.setName(generatedString);
			item.setStatus("NEW");
			mapper.save(item);
		}

		Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
        eav.put(":val1", new AttributeValue().withN("256"));
        DynamoDBQueryExpression<TestObject> queryExpression = new DynamoDBQueryExpression<TestObject>()
            .withKeyConditionExpression("empId = :val1").withExpressionAttributeValues(eav);

        List<TestObject> latestReplies = mapper.query(TestObject.class, queryExpression);

        for (TestObject reply : latestReplies) {
            System.out.println(reply.getName()+"---"+reply.getStatus());
        }
	}

}
