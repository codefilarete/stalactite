package org.codefilarete.stalactite.engine;

/**
 * @author Guillaume Mary
 */
public interface CompositeKeyMappingConfigurationProvider<C> {
	
	CompositeKeyMappingConfiguration<C> getConfiguration();
}