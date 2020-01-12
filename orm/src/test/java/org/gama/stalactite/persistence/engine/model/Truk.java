package org.gama.stalactite.persistence.engine.model;

import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;

/**
 * @author Guillaume Mary
 */
public class Truk extends Vehicle {
	
	public Truk() {
	}
	
	public Truk(Long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	Truk(Identifier<Long> id) {
		super(id);
	}
}
