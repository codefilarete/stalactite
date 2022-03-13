package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;

/**
 * @author Guillaume Mary
 */
public interface GroupByChain<SELF extends GroupByChain<SELF>> {
	
	SELF add(Column column, Column... columns);
	
	SELF add(String column, String... columns);
	
}
