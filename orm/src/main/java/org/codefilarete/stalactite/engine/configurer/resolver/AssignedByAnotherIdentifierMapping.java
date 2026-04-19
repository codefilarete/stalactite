package org.codefilarete.stalactite.engine.configurer.resolver;

import org.codefilarete.stalactite.engine.configurer.model.IdentifierMapping;

public class AssignedByAnotherIdentifierMapping<C, I> extends IdentifierMapping<C, I> {
	
	private final IdentifierMapping<C, I> source;
	
	public AssignedByAnotherIdentifierMapping(IdentifierMapping<C, I> source) {
		super(source.getIdAccessor(), source.getIdentifierInsertionManager());
		this.source = source;
	}
	
	public IdentifierMapping<C, I> getSource() {
		return source;
	}
}
