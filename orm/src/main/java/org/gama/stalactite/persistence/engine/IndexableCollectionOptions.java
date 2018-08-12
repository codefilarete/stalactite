package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.structure.Column;

/**
 * @author Guillaume Mary
 */
public interface IndexableCollectionOptions<T extends Identified, I extends StatefullIdentifier, O extends Identified> {
	
	/**
	 * Defines the indexing column of the mapped {@link java.util.List}.
	 * @param orderingColumn indexing column of the mapped {@link java.util.List}
	 * @return the global mapping configurer
	 */
	IndexableCollectionOptions<T, I, O> indexedBy(Column orderingColumn);
}
