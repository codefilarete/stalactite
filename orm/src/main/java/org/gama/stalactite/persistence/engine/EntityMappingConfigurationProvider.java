package org.gama.stalactite.persistence.engine;

/**
 * @author Guillaume Mary
 */
public interface EntityMappingConfigurationProvider<C, I> {
	
	EntityMappingConfiguration<C, I> getConfiguration();
}
