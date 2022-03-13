package org.codefilarete.stalactite.engine;

/**
 * @author Guillaume Mary
 */
public interface EntityMappingConfigurationProvider<C, I> {
	
	EntityMappingConfiguration<C, I> getConfiguration();
}
