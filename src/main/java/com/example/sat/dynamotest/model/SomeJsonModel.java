package com.example.sat.dynamotest.model;

public class SomeJsonModel {

	private String firstName;
	private String lastName;
	private EmbeddedJson embeddedJSON;
	
	public EmbeddedJson getEmbeddedJSON() {
		return embeddedJSON;
	}
	public void setEmbeddedJSON(EmbeddedJson embeddedJSON) {
		this.embeddedJSON = embeddedJSON;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	
}
