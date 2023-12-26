package org.codefilarete.stalactite.engine.model;

import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;

/**
 * @author Guillaume Mary
 */
public class City implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private String name;
	
	private Country country;
	
	private Set<Person> persons;
	
	private State state;
	
	public City() {
	}
	
	public City(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public City(Identifier<Long> id) {
		this.id = id;
	}
	
	public City(long id, String name) {
		this(id);
		setName(name);
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
		if (o == null || !this.getClass().isAssignableFrom(o.getClass())) {
			return false;
		}
		
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
	
	public AbstractCountry getAbstractCountry() {
		return country;
	}
	
	public void setCountry(Country country) {
		this.country = country;
	}
	
	public Set<Person> getPersons() {
		return persons;
	}
	
	public void setPersons(Set<Person> persons) {
		this.persons = persons;
	}
	
	public State getState() {
		return state;
	}
	
	public void setState(State state) {
		this.state = state;
	}
	
	/**
	 * Implemented for easier debug
	 *
	 * @return a simple representation of this
	 */
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
