package org.codefilarete.stalactite.engine.model;

import java.util.HashSet;
import java.util.Set;

import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;

/**
 * @author Guillaume Mary
 */
public class State implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private String name;
	
	private Country country;
	
	private Set<City> cities = new HashSet<>();
	
	public State() {
	}
	
	public State(Identifier<Long> id) {
		this.id = id;
	}
	
	@Override
	public Identifier<Long> getId() {
		return id;
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
		if (!(o instanceof State)) return false;
		
		State state = (State) o;
		
		return id.getDelegate().equals(state.id.getDelegate());
	}
	
	/**
	 * Implementation based on id, for everything that needs a hash
	 *
	 * @return id hashcode
	 */
	@Override
	public int hashCode() {
		return id.getDelegate().hashCode();
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public void setCountry(Country country) {
		this.country = country;
	}
	
	public Country getCountry() {
		return country;
	}
	
	public Set<City> getCities() {
		return cities;
	}
	
	public void setCities(Set<City> cities) {
		this.cities = cities;
	}
	
	@Override
	public String toString() {
		return "State{id=" + id +'}';
	}
}
