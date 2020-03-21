package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration.Linkage;

/**
 * Specialized version of {@link Linkage} for entity use case
 * 
 * @param <T>
 */
interface EntityLinkage<T> extends Linkage<T> {
	
	boolean isPrimaryKey();
}
