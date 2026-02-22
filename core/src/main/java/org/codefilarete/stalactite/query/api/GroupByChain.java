package org.codefilarete.stalactite.query.api;

import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * @author Guillaume Mary
 */
public interface GroupByChain<SELF extends GroupByChain<SELF>> {
	
	SELF add(Column column, Column... columns);
	
	SELF add(String column, String... columns);
	
}
