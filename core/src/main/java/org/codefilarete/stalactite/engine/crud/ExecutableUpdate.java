package org.codefilarete.stalactite.engine.crud;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * 
 * @param <T> table type
 * @author Guillaume Mary
 */
public interface ExecutableUpdate<T extends Table<T>> {
	
	/**
	 * Adds a column to update with its value.
	 *
	 * @param column any column
	 * @param value value for given column
	 * @param <O> value type
	 * @return this
	 */
	<O> ExecutableUpdate<T> set(Column<? extends T, O> column, O value);
	
	/**
	 * Executes this update statement with given values
	 */
	long execute();
}
