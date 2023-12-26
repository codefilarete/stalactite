package org.codefilarete.stalactite.engine.model;

import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.Identifier;

/**
 * @author Guillaume Mary
 */
public class Truck extends Vehicle {
	
	public Truck() {
	}
	
	public Truck(Long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	Truck(Identifier<Long> id) {
		super(id);
	}
}
