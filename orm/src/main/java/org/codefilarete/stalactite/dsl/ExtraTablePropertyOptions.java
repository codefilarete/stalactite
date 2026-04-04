package org.codefilarete.stalactite.dsl;

import org.codefilarete.stalactite.dsl.property.PropertyOptions;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Dedicated options for properties that need to be stored on a secondary / extra table.
 * Extra tables will be joined with main one on primary key.
 * 
 * @author Guillaume Mary
 */
public interface ExtraTablePropertyOptions {
	
	/**
	 * Indicates a secondary table on which the property should be stored.
	 * Will be joined with main one on primary key.
	 *
	 * @param name table name to use for property storage
	 */
	PropertyOptions extraTable(String name);
	
	/**
	 * Indicates a secondary table on which the property should be stored.
	 * Will be joined with main one on primary key.
	 *
	 * @param table table to use for property storage
	 */
	PropertyOptions extraTable(Table table);
}
