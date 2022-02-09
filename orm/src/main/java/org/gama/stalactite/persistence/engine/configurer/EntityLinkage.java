package org.codefilarete.stalactite.persistence.engine.configurer;

import org.codefilarete.stalactite.persistence.engine.EmbeddableMappingConfiguration.Linkage;

/**
 * Specialized version of {@link Linkage} for entity use case
 * 
 * @param <T>
 */
interface EntityLinkage<T> extends Linkage<T> {
	
	boolean isPrimaryKey();
}
