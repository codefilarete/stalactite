package org.codefilarete.stalactite.engine.configurer.entity;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import javax.annotation.Nullable;

/**
 * Stores information of {@link EntityMappingConfiguration.InheritanceConfiguration}
 *
 * @param <E> entity type
 * @param <I> identifier type
 */
class InheritanceConfigurationSupport<E, I> implements EntityMappingConfiguration.InheritanceConfiguration<E, I> {
	
	private final EntityMappingConfiguration<E, I> configuration;
	
	private boolean joinTable = false;
	
	@Nullable
	private Table table;
	
	InheritanceConfigurationSupport(EntityMappingConfiguration<E, I> configuration) {
		this.configuration = configuration;
	}
	
	@Override
	public EntityMappingConfiguration<E, I> getConfiguration() {
		return configuration;
	}
	
	public void setJoinTable(boolean joinTable) {
		this.joinTable = joinTable;
	}
	
	@Override
	public boolean isJoinTable() {
		return this.joinTable;
	}
	
	@Override
	@Nullable
	public Table getTable() {
		return this.table;
	}
	
	public void setTable(Table table) {
		this.table = table;
	}
	
	public void setTable(String parentTableName) {
		setTable(new Table(parentTableName));
	}
}
