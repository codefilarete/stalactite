package org.codefilarete.stalactite.engine;

/**
 * 
 * @author Guillaume Mary
 */
public interface SubEntityMappingConfigurationProvider<C> {
	
	SubEntityMappingConfiguration<C> getConfiguration();
}
