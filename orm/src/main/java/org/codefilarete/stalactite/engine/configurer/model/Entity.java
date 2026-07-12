package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class Entity<C, I, T extends Table<T>> extends AbstractEntity<C, I, T> {
	
	public Entity(IdentifierMapping<C, I> identifierMapping, Mapping<C, T> mapping) {
		super(identifierMapping, mapping);
	}
	
	@Override
	public boolean isTablePerClass() {
		return false;
	}
}
