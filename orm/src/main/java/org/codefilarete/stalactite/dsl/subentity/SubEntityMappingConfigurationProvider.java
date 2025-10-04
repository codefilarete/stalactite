package org.codefilarete.stalactite.dsl.subentity;

/**
 * 
 * @author Guillaume Mary
 */
public interface SubEntityMappingConfigurationProvider<C> {
	
	SubEntityMappingConfiguration<C> getConfiguration();
}
