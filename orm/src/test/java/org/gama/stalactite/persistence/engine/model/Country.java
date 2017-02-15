package org.gama.stalactite.persistence.engine.model;

import java.util.HashSet;
import java.util.Set;

import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;

/**
 * @author Guillaume Mary
 */
public class Country implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private String name;
	
	private String description;
	
	private Person president;
	
	private Set<City> cities = new HashSet<>();
	
	public Country() {
	}
	
	public Country(Identifier<Long> id) {
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
		if (!(o instanceof Country)) return false;
		
		Country country = (Country) o;
		
		return id.getSurrogate().equals(country.id.getSurrogate());
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
	
	public void setDescription(String description) {
		this.description = description;
	}
	
	public String getDescription() {
		return description;
	}
	
	public Person getPresident() {
		return president;
	}
	
	public void setPresident(Person president) {
		this.president = president;
	}
	
	public Set<City> getCities() {
		return cities;
	}
	
	public void setCities(Set<City> cities) {
		this.cities = cities;
	}
	
	public void addCity(City city) {
		this.cities.add(city);
		city.setCountry(this);
	}
}
