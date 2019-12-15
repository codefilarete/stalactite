package org.gama.stalactite.persistence.engine;

/**
 * @author Guillaume Mary
 */
public interface EmbeddableMappingConfigurationProvider<C> {
	
	EmbeddableMappingConfiguration<C> getConfiguration();
}
