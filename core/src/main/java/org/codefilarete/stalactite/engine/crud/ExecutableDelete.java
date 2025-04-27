package org.codefilarete.stalactite.engine.crud;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * 
 * @param <T> table type
 * @author Guillaume Mary
 */
public interface ExecutableDelete<T extends Table<T>> {
	
	/**
	 * Executes this delete statement with given values.
	 */
	long execute();
}
