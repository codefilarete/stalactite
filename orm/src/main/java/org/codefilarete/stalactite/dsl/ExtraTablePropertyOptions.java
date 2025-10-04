package org.codefilarete.stalactite.dsl;

import org.codefilarete.stalactite.dsl.property.PropertyOptions;

/**
 * Dedicated options for properties that need to be stored on a secondary / extra table.
 * Extra tables will be joined with main one on primary key.
 * 
 * @author Guillaume Mary
 */
public interface ExtraTablePropertyOptions {
	
	/**
	 * Indicates a secondary table on which the property should be stored
	 *
	 * @param name table name to use for property storage
	 */
	PropertyOptions extraTableName(String name);
}
