package org.codefilarete.stalactite.engine;

/**
 * @author Guillaume Mary
 */
public interface EmbeddableMappingConfigurationProvider<C> {
	
	EmbeddableMappingConfiguration<C> getConfiguration();
}
