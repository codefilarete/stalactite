package org.codefilarete.stalactite.dsl.key;

/**
 * @author Guillaume Mary
 */
public interface CompositeKeyMappingConfigurationProvider<C> {
	
	CompositeKeyMappingConfiguration<C> getConfiguration();
}