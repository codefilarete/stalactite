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
	
	/**
	 * Implementation based on id, for any {@link java.util.Collection#contains(Object)} or {@link java.util.Collection#remove(Object)}
	 * 
	 * @param o the comparison object
	 * @return true if this equals the argument
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof City)) return false;
		
		City city = (City) o;
		
		return id.getSurrogate().equals(city.id.getSurrogate());
	}
	
	/**
	 * Implementation based on id, for everything that needs a hash
	 * 
	 * @return id hashcode
	 */
	@Override
	public int hashCode() {
		return id.getSurrogate().hashCode();
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
	
	@Override
	public String toString() {
		return "City{id=" + id + '}';
	}
}
