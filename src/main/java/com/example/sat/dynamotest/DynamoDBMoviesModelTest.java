package com.example.sat.dynamotest;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.example.sat.dynamotest.model.MoviesTable;
import com.example.sat.dynamotest.model.SomeJsonModel;
import com.fasterxml.jackson.databind.ObjectMapper;

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
		
		MoviesTable item = new MoviesTable();
		item.setName("Using DynamoDBMapper");
		item.setRating("***");
		item.setSomeJson(someJsonObj);
		item.setFans(new ArrayList<String>(Arrays.asList("King","Kong")));
		item.setYear("1989");
		mapper.save(item);          
		
		String queryName = "Using DynamoDBMapper";
		Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
        eav.put(":val1", new AttributeValue().withS(queryName.toString()));
        DynamoDBQueryExpression<MoviesTable> queryExpression = new DynamoDBQueryExpression<MoviesTable>()
            .withKeyConditionExpression("movie_name = :val1").withExpressionAttributeValues(eav);

        List<MoviesTable> latestReplies = mapper.query(MoviesTable.class, queryExpression);

        for (MoviesTable reply : latestReplies) {
            System.out.println(reply.getSomeJson());
        }
	}

}
