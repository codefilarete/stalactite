package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;

public abstract class IdentifierMapping<C, I> {
	
	private final ReadWritePropertyAccessPoint<C, I> idAccessor;
	
	private final IdentifierInsertionManager<C, I> identifierInsertionManager;
	
	public IdentifierMapping(ReadWritePropertyAccessPoint<C, I> idAccessor, IdentifierInsertionManager<C, I> identifierInsertionManager) {
		this.idAccessor = idAccessor;
		this.identifierInsertionManager = identifierInsertionManager;
	}
	
	public ReadWritePropertyAccessPoint<C, I> getIdAccessor() {
		return idAccessor;
	}
	
	public IdentifierInsertionManager<C, I> getIdentifierInsertionManager() {
		return identifierInsertionManager;
	}
}
