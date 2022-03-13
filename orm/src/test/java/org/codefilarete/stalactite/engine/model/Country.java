package org.codefilarete.stalactite.engine.model;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.codefilarete.stalactite.engine.OneToManyOptions;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;

/**
 * @author Guillaume Mary
 */
public class Country extends AbstractCountry implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private String name;
	
	private String description;
	
	private Person president;
	
	private City capital;
	
	/** Country cities, lazily initialized to test initialization by Stalactite with {@link OneToManyOptions#initializeWith(Supplier)} */
	private Set<City> cities;
	
	// anything that is a List with a reverse relation-owning column
	private List<City> ancientCities = new ArrayList<>();
	
	private Set<State> states = new HashSet<>();
	
	private int version;
	
	private LocalDateTime modificationDate;
	
	private Timestamp timestamp;
	
	public Country() {
	}
	
	public Country(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Country(Identifier<Long> id) {
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
		if (o == null || !this.getClass().isAssignableFrom(o.getClass())) return false;
		
		Country country = (Country) o;
		
		return id.getSurrogate().equals(country.id.getSurrogate());
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
	
	public City getCapital() {
		return capital;
	}
	
	public void setCapital(City capital) {
		if (this.capital != null) {
			this.capital.setCountry(null);
		}
		this.capital = capital;
		if (this.capital != null) {
			this.capital.setCountry(this);
		}
	}
	
	public Set<City> getCities() {
		return cities;
	}
	
	public void setCities(Set<City> cities) {
		this.cities = cities;
	}
	
	public void addCity(City city) {
		if (cities == null) {
			cities = new HashSet<>();
		}
		this.cities.add(city);
		city.setCountry(this);
	}
	
	public List<City> getAncientCities() {
		return ancientCities;
	}
	
	public void setAncientCities(List<City> ancientCities) {
		this.ancientCities = ancientCities;
	}
	
	public void addAncientCity(City city) {
		this.ancientCities.add(city);
		city.setCountry(this);
	}
	
	public Set<State> getStates() {
		return states;
	}
	
	public void setStates(Set<State> states) {
		this.states = states;
	}
	
	public void addState(State state) {
		this.states.add(state);
		state.setCountry(this);
	}
	
	public int getVersion() {
		return version;
	}
	
	public LocalDateTime getModificationDate() {
		return modificationDate;
	}
	
	public Timestamp getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
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
