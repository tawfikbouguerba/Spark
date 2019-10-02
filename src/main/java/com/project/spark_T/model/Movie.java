package com.project.spark_T.model;

import java.io.Serializable;

public class Movie implements Serializable {

private String name;
private Double rating;
private String timestamp;

public Movie(String name, Double rating, String timestamp) {
	
				super();
				this.name = name;
				this.rating = rating;
				this.timestamp = timestamp;
			}
			
			public String getName() {
			  return name;
			}
			
			public void setName(String name) {
			   this.name = name;
			}
			
			public Double getRating() {
			   return rating;
			}
			
			public void setRating(Double rating) {
			   this.rating = rating;
			}
			
			public String gettimestamp() {
			   return timestamp;
			}
			
			public void settimestamp(String timestamp) {
			   this.timestamp= timestamp;
			}
	}