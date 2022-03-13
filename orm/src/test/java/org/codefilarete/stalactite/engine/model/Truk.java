package org.codefilarete.stalactite.engine.model;

import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.Identifier;

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
