package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class PolymorphicEntity<C, I, T extends Table<T>> extends AbstractEntity<C, I, T> {
	
	private final EntityPolymorphism<C, I> polymorphism;
	
	public PolymorphicEntity(IdentifierMapping<C, I> identifierMapping, Mapping<C, T> mapping, EntityPolymorphism<C, I> polymorphism) {
		super(identifierMapping, mapping);
		this.polymorphism = polymorphism;
	}
	
	public EntityPolymorphism<C, I> getPolymorphism() {
		return polymorphism;
	}
	
	@Override
	public boolean isTablePerClass() {
		return this.polymorphism instanceof TablePerClassPolymorphism;
	}
}
