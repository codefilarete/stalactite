package org.codefilarete.stalactite.persistence.engine.model;

import org.codefilarete.stalactite.persistence.id.Identifier;

/**
 * 
 * @author Guillaume Mary
 */
public class PersonWithGender extends Person {
	
	private Gender gender;
	
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