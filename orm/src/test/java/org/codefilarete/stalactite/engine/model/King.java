package org.codefilarete.stalactite.engine.model;

import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;

public class King implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private String name;
	
	public King() {
	}
	
	public King(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public King(Identifier<Long> id) {
		this.id = id;
	}
	
	@Override
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
