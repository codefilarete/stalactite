package org.gama.stalactite.persistence.engine.model;

import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;

/**
 * @author Guillaume Mary
 */
public class Person implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private String name;
	
	private int version;
	
	public Person() {
	}
	
	public Person(Identifier<Long> id) {
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
	
	public int getVersion() {
		return version;
	}
}
