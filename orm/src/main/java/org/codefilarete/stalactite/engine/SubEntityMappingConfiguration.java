package org.codefilarete.stalactite.engine;

import javax.annotation.Nullable;

/**
 * Configuration dedicated to polymorphism : in such cases sub-entities don't define identifier policy because it is done by the polymorphic
 * type itself. Hence, it is all what an EntityMappingConfiguration can do, without identification information. 
 * 
 * @author Guillaume Mary
 */
public interface SubEntityMappingConfiguration<C> extends RelationalMappingConfiguration<C> {
	
	EmbeddableMappingConfiguration<C> getPropertiesMapping();
	
	@Nullable
	PolymorphismPolicy<C> getPolymorphismPolicy();
}
