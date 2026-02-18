package org.codefilarete.stalactite.engine.model.restaurant;

import org.codefilarete.tool.collection.Arrays;

import java.util.HashSet;
import java.util.Set;

public class Restaurant {
	
	private long id;
	private String name;
	private String description;
	private Set<Review> reviews = new HashSet<>();
	
	public Restaurant(long id) {
		this.id = id;
	}
	
	public Restaurant(String name, String description) {
		this.name = name;
		this.description = description;
	}
	
	public long getId() {
		return id;
	}
	
	public void setId(long id) {
		this.id = id;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getDescription() {
		return description;
	}
	
	public void setDescription(String description) {
		this.description = description;
	}
	
	public Set<Review> getReviews() {
		return reviews;
	}
	
	public void setReviews(Set<Review> reviews) {
		this.reviews = reviews;
	}
	
	public void addReviews(Review... reviews) {
		this.reviews.addAll(Arrays.asList(reviews));
		for (Review review : reviews) {
			review.setRestaurant(this);
		}
	}
}
