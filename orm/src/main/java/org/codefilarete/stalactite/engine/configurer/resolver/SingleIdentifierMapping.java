package org.codefilarete.stalactite.engine.configurer.resolver;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.configurer.model.IdentifierMapping;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;

public class SingleIdentifierMapping<C, I> extends IdentifierMapping<C, I> {
	
	public SingleIdentifierMapping(ReadWritePropertyAccessPoint<C, I> idAccessor, IdentifierInsertionManager<C, I> identifierInsertionManager) {
		super(idAccessor, identifierInsertionManager);
	}
}
