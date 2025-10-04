package org.codefilarete.stalactite.dsl.embeddable;

/**
 * @author Guillaume Mary
 */
public interface EmbeddableMappingConfigurationProvider<C> {
	
	EmbeddableMappingConfiguration<C> getConfiguration();
}
