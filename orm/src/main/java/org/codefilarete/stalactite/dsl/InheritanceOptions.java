package org.codefilarete.stalactite.dsl;

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
	InheritanceOptions joiningTables();
	
}
