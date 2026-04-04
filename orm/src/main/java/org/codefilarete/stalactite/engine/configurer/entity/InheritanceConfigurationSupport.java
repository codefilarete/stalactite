package org.codefilarete.stalactite.engine.configurer.entity;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.InheritanceConfiguration;

/**
 * Stores information of {@link InheritanceConfiguration}
 *
 * @param <E> entity type
 * @param <I> identifier type
 */
class InheritanceConfigurationSupport<E, I> implements InheritanceConfiguration<E, I> {
	
	private final EntityMappingConfiguration<E, I> superConfiguration;
	
	private boolean joiningTables = false;
	
	InheritanceConfigurationSupport(EntityMappingConfiguration<E, I> superConfiguration) {
		this.superConfiguration = superConfiguration;
	}
	
	@Override
	public EntityMappingConfiguration<E, I> getParentMappingConfiguration() {
		return superConfiguration;
	}
	
	public void setJoiningTables(boolean joiningTables) {
		this.joiningTables = joiningTables;
	}
	
	@Override
	public boolean isJoiningTables() {
		return this.joiningTables;
	}
}
