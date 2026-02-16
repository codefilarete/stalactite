package org.codefilarete.stalactite.dsl;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Contract for inheritance options
 * 
 * @author Guillaume Mary
 */
public interface InheritanceOptions {
	
	/**
	 * Mark the parent entity to be on a separate table, joined with the current entity table on primary keys.
	 * 
	 * @return an instance that allows method chaining
	 */
	InheritanceOptions withJoinedTable();
	
	/**
	 * Specifies the table on which to store the parent entity. The current entity table will be joined with it on primary keys.
	 *
	 * @return an instance that allows method chaining
	 */
	InheritanceOptions withJoinedTable(Table parentTable);
	
	/**
	 * Specifies the table name on which to store the parent entity. The current entity table will be joined with it on primary keys.
	 *
	 * @return an instance that allows method chaining
	 */
	InheritanceOptions withJoinedTable(String parentTableName);
	
}
