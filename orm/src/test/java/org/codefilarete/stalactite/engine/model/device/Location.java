package org.codefilarete.stalactite.engine.model.device;

import java.util.Set;

import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;

public abstract class Location implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private Country country;
	
	private Set<Review> reviews;
	
	public Location() {
	}
	
	public Location(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Location(Identifier<Long> id) {
		this.id = id;
	}
	
	public Identifier<Long> getId() {
		return id;
	}
	
	public Country getCountry() {
		return country;
	}
	
	public void setCountry(Country country) {
		this.country = country;
	}
	
	public Set<Review> getReviews() {
		return reviews;
	}
	
	public void setReviews(Set<Review> reviews) {
		this.reviews = reviews;
	}
}
