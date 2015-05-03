package org.gama.reflection.model;

import java.util.Collection;

/**
 * @author Guillaume Mary
 */
public class Address {
	
	private City city;
	
	private Collection<Phone> phones;
	
	public Address(City city, Collection<Phone> phones) {
		this.city = city;
		this.phones = phones;
	}
}
