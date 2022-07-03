package org.codefilarete.stalactite.engine.model;

public abstract class Element {
	
	private long id;
	
	protected Element(long id) {
		this.id = id;
	}
	
	public long getId() {
		return id;
	}
}
