package org.codefilarete.stalactite.engine.model.device;

import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;

public class Company implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private String name;
	
	public Company() {
	}
	
	public Company(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Company(Identifier<Long> id) {
		this.id = id;
	}
	
	public Identifier<Long> getId() {
		return id;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
}
