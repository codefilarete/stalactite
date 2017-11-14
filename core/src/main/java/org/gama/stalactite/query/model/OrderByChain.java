package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public interface OrderByChain<SELF extends OrderByChain<SELF>> {
	
	SELF add(Column column, Column... columns);
	
	SELF add(String column, String... columns);
}
