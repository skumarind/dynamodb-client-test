package com.example.sat.dynamotest.model;

import java.util.List;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName="my-favorite-movies-table")
public class MoviesTable {
	@DynamoDBHashKey(attributeName="movie_name") 
	private String name;
	
	@DynamoDBAttribute(attributeName="fans")
    private List<String> fans;
    
	@DynamoDBAttribute(attributeName="rating")
	private String rating;
	
	@DynamoDBAttribute(attributeName="year")
    private String year;
	
	@DynamoDBAttribute(attributeName="someJson")
    private String someJson;
    
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getSomeJson() {
		return someJson;
	}
	public void setSomeJson(String someJson) {
		this.someJson = someJson;
	}
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	public String getRating() {
		return rating;
	}
	public void setRating(String rating) {
		this.rating = rating;
	}
	public List<String> getFans() {
		return fans;
	}
	public void setFans(List<String> fans) {
		this.fans = fans;
	}
    
    
}
