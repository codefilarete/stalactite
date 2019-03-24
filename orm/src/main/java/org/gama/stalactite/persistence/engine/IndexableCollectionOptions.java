package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.structure.Column;

/**
 * @author Guillaume Mary
 */
public interface IndexableCollectionOptions<T, I, O> {
	
	/**
	 * Defines the indexing column of the mapped {@link java.util.List}.
	 * @param orderingColumn indexing column of the mapped {@link java.util.List}
	 * @return the global mapping configurer
	 */
	IndexableCollectionOptions<T, I, O> indexedBy(Column orderingColumn);
}
