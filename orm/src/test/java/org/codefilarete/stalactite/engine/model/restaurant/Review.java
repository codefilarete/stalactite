package org.codefilarete.stalactite.engine.model.restaurant;

import java.time.LocalDate;

public class Review {
	
	private long id;
	private String comment;
	private int rating;
	private LocalDate date;
	private Restaurant restaurant;
	
	public Review(long id) {
		this.id = id;
	}
	
	public Review(String comment, int rating) {
		this.comment = comment;
		this.rating = rating;
	}
	
	public long getId() {
		return id;
	}
	
	public void setId(long id) {
		this.id = id;
	}
	
	public String getComment() {
		return comment;
	}
	
	public void setComment(String comment) {
		this.comment = comment;
	}
	
	public int getRating() {
		return rating;
	}
	
	public void setRating(int rating) {
		this.rating = rating;
	}
	
	public LocalDate getDate() {
		return date;
	}
	
	public void setDate(LocalDate date) {
		this.date = date;
	}
	
	public Restaurant getRestaurant() {
		return restaurant;
	}
	
	public void setRestaurant(Restaurant restaurant) {
		this.restaurant = restaurant;
	}
}
