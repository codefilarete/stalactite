package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * @author Guillaume Mary
 */
public interface IndexableCollectionOptions<C, I, O> {
	
	/**
	 * Defines the indexing column of the mapped {@link java.util.List}.
	 * @param orderingColumn indexing column of the mapped {@link java.util.List}
	 * @return the global mapping configurer
	 */
	IndexableCollectionOptions<C, I, O> indexedBy(Column<?, Integer> orderingColumn);
	
	IndexableCollectionOptions<C, I, O> indexedBy(String columnName);
}
