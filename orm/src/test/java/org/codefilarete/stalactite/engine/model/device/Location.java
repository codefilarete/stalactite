package org.codefilarete.stalactite.engine.model.device;

import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;

public abstract class Location {
	
	private Identifier<Long> id;
	
	public Location() {
	}
	
	public Location(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Location(Identifier<Long> id) {
		this.id = id;
	}
	
	public Identifier<Long> getId() {
		return id;
	}
}
