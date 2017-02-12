package org.gama.stalactite.persistence.engine.model;

import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;

/**
 * @author Guillaume Mary
 */
public class City implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private String name;
	
	private Country country;
	
	public City() {
	}
	
	public City(Identifier<Long> id) {
		this.id = id;
	}
	
	@Override
	public Identifier<Long> getId() {
		return id;
	}
	
	@Override
	public void setId(Identifier<Long> id) {
		this.id = id;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public Country getCountry() {
		return country;
	}
	
	public void setCountry(Country country) {
		this.country = country;
	}
}
