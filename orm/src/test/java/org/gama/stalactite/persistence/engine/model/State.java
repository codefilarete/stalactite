package org.gama.stalactite.persistence.engine.model;

import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;

/**
 * @author Guillaume Mary
 */
public class State implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private String name;
	
	private Country country;
	
	public State() {
	}
	
	public State(Identifier<Long> id) {
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
	 * Implemented for difference computation between Collection. See {@link org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer}
	 * @param o
	 * @return
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof State)) return false;
		
		State state = (State) o;
		
		return id.getSurrogate().equals(state.id.getSurrogate());
	}
	
	/**
	 * Implemented for difference computation between Collection. See {@link org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer}
	 * @return
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
	
	public void setCountry(Country country) {
		this.country = country;
	}
	
	public Country getCountry() {
		return country;
	}
}
