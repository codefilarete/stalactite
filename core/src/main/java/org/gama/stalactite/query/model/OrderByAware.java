package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public interface OrderByAware {
	
	OrderByChain orderBy(Column column, Column... columns);
	
	OrderByChain orderBy(String column, String... columns);
	
}
