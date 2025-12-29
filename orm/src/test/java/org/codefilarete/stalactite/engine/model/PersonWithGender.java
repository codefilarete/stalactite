package org.codefilarete.stalactite.engine.model;

import org.codefilarete.stalactite.id.Identifier;

/**
 * 
 * @author Guillaume Mary
 */
public class PersonWithGender extends Person {
	
	private Gender gender;
	
	private Gender fieldWithoutAccessor;
	
	public PersonWithGender() {
	}
	
	public PersonWithGender(Identifier<Long> identifier) {
		super(identifier);
	}
	
	public Gender getGender() {
		return gender;
	}
	
	public void setGender(Gender gender) {
		this.gender = gender;
	}
}