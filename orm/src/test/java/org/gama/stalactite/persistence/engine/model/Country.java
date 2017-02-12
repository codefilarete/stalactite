package org.gama.stalactite.persistence.engine.model;

import java.util.Collection;
import java.util.HashSet;

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
	
	private Collection<City> cities = new HashSet<>();
	
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
	
	public Collection<City> getCities() {
		return cities;
	}
	
	public void setCities(Collection<City> cities) {
		this.cities = cities;
	}
	
	public void addCity(City city) {
		this.cities.add(city);
		city.setCountry(this);
	}
}
