package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Column;

/**
 * @author Guillaume Mary
 */
public interface GroupByChain<SELF extends GroupByChain<SELF>> {
	
	SELF add(Column column, Column... columns);
	
	SELF add(String column, String... columns);
	
}
