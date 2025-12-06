package org.codefilarete.stalactite.engine.configurer.entity;

import java.util.function.Function;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;

class EntityFactoryProviderSupport<C, T extends Table> implements EntityMappingConfiguration.EntityFactoryProvider<C, T> {
	
	private final Function<Table, Function<ColumnedRow, C>> factory;
	
	private final boolean setIdentifier;
	
	EntityFactoryProviderSupport(Function<Table, Function<ColumnedRow, C>> factory, boolean setIdentifier) {
		this.factory = factory;
		this.setIdentifier = setIdentifier;
	}
	
	@Override
	public Function<ColumnedRow, C> giveEntityFactory(T table) {
		return factory.apply(table);
	}
	
	@Override
	public boolean isIdentifierSetByFactory() {
		return setIdentifier;
	}
}
