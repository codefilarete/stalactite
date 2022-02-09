package org.codefilarete.stalactite.persistence.engine;

/**
 * @author Guillaume Mary
 */
public interface EmbeddableMappingConfigurationProvider<C> {
	
	EmbeddableMappingConfiguration<C> getConfiguration();
}
