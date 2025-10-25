package org.codefilarete.stalactite.engine.model.device;

import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;

public class Address extends Location {
	
	private String street;
	
	private City city;
	
	public Address() {
	}
	
	public Address(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Address(Identifier<Long> id) {
		super(id);
	}
	
	public String getStreet() {
		return street;
	}
	
	public void setStreet(String street) {
		this.street = street;
	}
	
	public City getCity() {
		return city;
	}
	
	public void setCity(City city) {
		this.city = city;
	}
}
